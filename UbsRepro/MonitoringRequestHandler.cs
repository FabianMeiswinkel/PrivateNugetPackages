using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace UbsRepro
{
    /// <summary>
    /// RequestHandler pipeline that is emitting logs and metrics for all CosmosDB requests
    /// </summary>
    public class MonitoringRequestHandler : RequestHandler
    {
        private readonly Uri accountEndpoint;
        private readonly string accountName;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of <see cref="MonitoringRequestHandler" /> with the specified
        /// account endpoint. Logs and metrics will be emitted for the specified listeners.
        /// </summary>
        /// <param name="accountEndpoint">
        /// The CosmosDB account endpoint
        /// </param>
        /// <param name="listeners">
        /// The collection of listeners logs and metrics will be emitted to.
        /// </param>
        public MonitoringRequestHandler(
            Uri accountEndpoint,
            ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }
            if (accountEndpoint == null) { throw new ArgumentNullException(nameof(accountEndpoint)); }
            if (!accountEndpoint.IsAbsoluteUri) { throw new ArgumentOutOfRangeException(nameof(accountEndpoint)); }

            this.accountEndpoint = accountEndpoint;
            this.accountName = accountEndpoint.Host;
            this.logger = logger;
        }

        /// <summary>
        /// Processes the current <see cref="RequestMessage" /> in the current handler and sends the
        /// current <see cref="RequestMessage" /> to the next handler in the chain.
        /// </summary>
        /// <param name="request">
        /// <see cref="RequestMessage" /> received by the handler.
        /// </param>
        /// <param name="cancellationToken">
        /// <see cref="CancellationToken" /> received by the handler.
        /// </param>
        /// <returns>
        /// An instance of <see cref="ResponseMessage" />.
        /// </returns>
        public override async Task<ResponseMessage> SendAsync(
            RequestMessage request,
            CancellationToken cancellationToken)
        {
            ResponseMessage response = null;
            try
            {
                response = await base.SendAsync(request, cancellationToken);

                return response;
            }
            finally
            {
                this.PublishRequestDiagnostics(response);
            }
        }

        private void PublishRequestDiagnostics(ResponseMessage response)
        {
            if (response == null ||
                response.Diagnostics == null)
            {
                return;
            }

            var diagnostics = response.Diagnostics.ToString();
            if (String.IsNullOrWhiteSpace(diagnostics))
            {
                return;
            }

            var resource = "UNKNOWN";
            var operation = "UNKNOWN";
            var activityId = response.Headers?.ActivityId;

            if (response.RequestMessage != null)
            {
                operation = response.RequestMessage.Method.ToString();

                resource = response.RequestMessage.RequestUri.IsAbsoluteUri
                    ? response.RequestMessage.RequestUri.MakeRelativeUri(this.accountEndpoint).ToString()
                    : response.RequestMessage.RequestUri.ToString();

                if (response.RequestMessage.Headers.ContentType == "application/query+json")
                {
                    operation = OperationNames.QueryPageRequest;
                }
            }

            RequestDiagnosticsContract requestDiagnostics =
                JsonSerializer.Deserialize<RequestDiagnosticsContract>(diagnostics);

            double? latencyMS = null;

            if (!String.IsNullOrWhiteSpace(requestDiagnostics.RequestLatency) &&
                TimeSpan.TryParse(requestDiagnostics.RequestLatency, out TimeSpan latency))
            {
                latencyMS = latency.TotalMilliseconds;
                   }

            this.LogRequestDiagnostics(
                this.accountName,
                resource,
                operation,
                activityId,
                (int)response.StatusCode,
                latencyMS,
                diagnostics);
        }

        private void LogRequestDiagnostics(
            string account,
            string resource,
            string operation,
            string activityId,
            int statusCode,
            double? latencyMS,
            string diagnostics)
        {
            var latencyMSFormatted = latencyMS != null ?
                    latencyMS.Value.ToString("#,###,##0.00", CultureInfo.InvariantCulture) :
                    String.Empty;

            this.logger.LogInformation(
                "LogRequest->{0}|{1}|{2}|{3}|{4}",
                operation,
                activityId,
                statusCode,
                latencyMSFormatted,
                diagnostics);
        }

        private class RequestDiagnosticsContract
        {
            public string ActivityId { get; set; }

            [JsonPropertyName("requestLatency")]
            public string RequestLatency { get; set; }
        }

        /// <summary>
        /// Constant operation names used in metrics and logs
        /// </summary>
        public static class OperationNames
        {
            /// <summary>
            /// Operation name for query metrics and logs
            /// </summary>
            public const string Query = "QUERY";

            /// <summary>
            /// Operation name for query page metrics and logs
            /// </summary>
            public const string QueryPage = "QUERYPAGE";

            /// <summary>
            /// Operation name for query page request metrics and logs
            /// </summary>
            public const string QueryPageRequest = "QUERYPAGEREQUEST";
        }
    }
}
