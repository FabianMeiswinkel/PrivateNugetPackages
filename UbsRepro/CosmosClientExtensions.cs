using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace UbsRepro
{
    public static class CosmosClientExtensions
    {
        public async static Task<string> GetSelfLinkAsync(
            this Container container,
            CancellationToken cancellationToken = default)
        {
            if (container == null) { throw new ArgumentNullException(nameof(container)); }

            using (ResponseMessage response =
                await container.ReadContainerStreamAsync(cancellationToken: cancellationToken))
            {
                return await ExtractSelfLinkAsync(response);
            }
        }

        public async static Task<string> GetSelfLinkAsync(
            this Database database,
            CancellationToken cancellationToken = default)
        {
            if (database == null) { throw new ArgumentNullException(nameof(database)); }

            using (ResponseMessage response =
                await database.ReadStreamAsync(cancellationToken: cancellationToken))
            {
                return await ExtractSelfLinkAsync(response);
            }
        }

        private static async Task<string> ExtractSelfLinkAsync(ResponseMessage response)
        {
            response.EnsureSuccessStatusCode();

            string json;

            using (StreamReader reader = new StreamReader(response.Content))
            {
                json = await reader.ReadToEndAsync();
            }

            if (String.IsNullOrWhiteSpace(json))
            {
                throw new InvalidDataException("Invalid metadata.");
            }

            try
            {
                JObject metadata = JObject.Parse(json);

                string selfLink = metadata["_self"].Value<string>();

                if (String.IsNullOrWhiteSpace(selfLink))
                {
                    throw new InvalidDataException("Invalid metadata.");
                }

                return selfLink;
            }
            catch (JsonReaderException error)
            {
                throw new InvalidDataException("Invalid metadata.", error);
            }
        }
    }
}
