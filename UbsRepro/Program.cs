using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace UbsRepro
{
    class Program
    {
        const string connectionStringName = "EngConnectionString";

        static int Main(string[] args)
        {
            return MainAsync(args).ConfigureAwait(continueOnCapturedContext: false).GetAwaiter().GetResult();
        }

        private static async Task<int> MainAsync(string[] args)
        {
            var connectionString = System.Environment.GetEnvironmentVariable(
                "EngConnectionString",
                EnvironmentVariableTarget.Process);

            if (String.IsNullOrWhiteSpace(connectionString))
            {
                Console.WriteLine($"ERROR - Environment variable '{connectionStringName}' " +
                    "not set to a valid connection string for the CosmosDB account.");
                Console.WriteLine("Press any key to quit...");
                Console.ReadKey();

                return 1;
            }

            IServiceCollection serviceCollection = new ServiceCollection();

            serviceCollection.AddLogging(builder => builder
                .AddConsole()
                .AddFilter(level => level >= LogLevel.Information)
            );

            ILoggerFactory loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();

            ILogger log = loggerFactory.CreateLogger("UbsRepro");

            var options = new CosmosClientOptions
            {
                ApplicationName = "UbsRepro",
                ConnectionMode = ConnectionMode.Direct,
            };

            //The monitoring handler ensures logging of request diagnostics for all requests made

            //by the CosmosClient instance and is also emitting metrics for 'RequestCount',
            //'RequestCharge' and 'LatencyMs'

           var monitoringRequestHandler = new MonitoringRequestHandler(
               connectionString.GetAccountEndpointFromConnectionString(),
               log);
           options.CustomHandlers.Add(monitoringRequestHandler);

            try
            {
                for (int i = 0; i < 1; i++)
                {
                    Task[] tasks = new Task[1];

                    for (int c = 0; c < tasks.Length; c++)
                    {
                        var testId = Guid.NewGuid().ToString("N");
                        options.ApplicationName = $"UbsRepro-{testId}";

                        Console.WriteLine($"TEST ITERATION {i} - Test ID: {testId}");
                        tasks[c] = TestAsync(connectionString, options, testId);
                    }

                    await Task.WhenAll(tasks);
                }

                Console.WriteLine("*** Complected successfully ***- press any key to quit...");
                Console.WriteLine("Press any key to quit...");
                Console.ReadKey();
            }
            catch (Exception error)
            {
                log.LogError(error, "Unhandled exception occurred. {0}", error);
                Console.WriteLine("UNHANDLED EXCEPTION: {0}", error);
                Console.WriteLine("Press any key to quit...");
                Console.ReadKey();

                return 1;
            }

            return 0;
        }

        private static async Task TestAsync(
            string connectionString,
            CosmosClientOptions options,
            string testId)
        {
            using (var cosmosClient = new CosmosClient(connectionString, options))
            {
                var database = cosmosClient.GetDatabase("vistore");
                Console.WriteLine(
                    "Database SelfLink: {0}",
                    await database.GetSelfLinkAsync());

                var container = database.GetContainer("LEGACY_BROIL_TRADE|EOD_EMEA");
                Console.WriteLine(
                    "Container SelfLink: {0}", 
                    await container.GetSelfLinkAsync());

                dynamic doc = new JObject();
                doc.header = new JObject() as dynamic;
                doc.header.dataType = "LEGACY_BROIL_TRADE";
                doc.header.label = "EOD_EMEA";
                doc.header.key = "95105017";
                doc.header.validFrom = 1564402733;
                doc.header.expiry = 1584662400;
                doc.header.metaData = new JObject() as dynamic;


                doc.header.metaData.enqueueTimestamp = new DateTime(2019, 11, 13, 12, 17, 43);

                doc.header.metaData.isacSwcId = "AA32355";
                doc.header.metaData.isacSwcName = "RATES BROIL PROD";
                doc.header.metaData.isacSwciId = "AT3659";
                doc.header.metaData.isacSwciName = "RATES BROIL PROD";
                doc.header.metaData.source = null;
                doc.header.metaData.topicName = "aen-eng-neu-df-broil-tradefeed";

                doc.id = $"TEST-{testId}";
                doc.payload = new JObject() as dynamic;
                doc.payload.blob = "<? xml version =\"1.0\" encoding=\"UTF-8\"?><Trade>\n<DealIdentity>\n<DEAL_TICKETNUM>123</DEAL_TICKETNUM>\n<REVISION_LEVEL>0</REVISION_LEVEL>\n<LEG_ID>0</LEG_ID>\n<DEALID>123</DEALID>\n</DealIdentity>\n<DealHeader>\n<GroupInformation>\n<DEAL_GROUP_BY>0</DEAL_GROUP_BY>\n</GroupInformation>\n<AdminInfo>\n<RISKCL>OE04</RISKCL>\n<TRADER>XYZ, ABC</TRADER>\n</AdminInfo>\n<DefaultValuationInfo>\n<GPL_MODEL>SABR_EUR_1_FCD</GPL_MODEL>\n<GPL_VAL_METH>CLOSED FORM</GPL_VAL_METH>\n</DefaultValuationInfo>\n<ConfirmInfo>\n<TRADE_DATE>2019-07-29</TRADE_DATE>\n<COUNTERPARTY_ID>0</COUNTERPARTY_ID>\n</ConfirmInfo>\n</DealHeader>\n<DealFinancialInformation>\n<CollateralType>NONE</CollateralType>\n<BroilHedgeTrade>\n<TradeCurrencies>\n<ValueCurrency>EUR</ValueCurrency>\n</TradeCurrencies>\n<EuroLiborFuture TradeName=\"OE04_95105017\">\n<FutureExpiryInformation>\n<ExpiryCode>MAR 20</ExpiryCode>\n</FutureExpiryInformation>\n<BuyOrSell>BUY</BuyOrSell>\n<NumberOfContracts>158</NumberOfContracts>\n<PriceTraded>123</PriceTraded>\n</EuroLiborFuture>\n</BroilHedgeTrade>\n</DealFinancialInformation>\n</Trade>\n";

                await container.CreateItemAsync(doc, new PartitionKey("95105017"));

            }
        }
    }
}
