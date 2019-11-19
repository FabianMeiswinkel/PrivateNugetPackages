using System;
using System.Data.Common;

namespace UbsRepro
{
    /// <summary>
    /// Extension methods to extract values from the CosmosDB connection string
    /// </summary>
    public static class ConnectionStringExtensions
    {
        private const string ConnectionStringAccountEndpoint = "AccountEndpoint";

        /// <summary>
        /// Returns the account endpoint from the CosmosDB connection string
        /// </summary>
        /// <param name="connectionString">
        /// The CosmosDB connection string
        /// </param>
        /// <returns>
        /// Returns the account endpoint from the CosmosDB connection string
        /// </returns>
        public static Uri GetAccountEndpointFromConnectionString(this string connectionString)
        {
            return new Uri(GetValueFromConnectionString(connectionString, ConnectionStringAccountEndpoint));
        }

        /// <summary>
        /// Returns the account name from the CosmosDB connection string
        /// </summary>
        /// <param name="connectionString">
        /// The CosmosDB connection string
        /// </param>
        /// <returns>
        /// Returns the account name from the CosmosDB connection string
        /// </returns>
        public static string GetAccountNameFromConnectionString(this string connectionString)
        {
            var host = GetAccountEndpointFromConnectionString(connectionString).Host ?? String.Empty;

            var seperatorIndex = host.IndexOf(".", StringComparison.Ordinal);
            return seperatorIndex == -1 ? host : host.Substring(0, seperatorIndex);
        }

        private static string GetValueFromConnectionString(string connectionString, string keyName)
        {
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            if (builder.TryGetValue(keyName, out var value))
            {
                var keyNameValue = value as string;
                if (!String.IsNullOrEmpty(keyNameValue))
                {
                    return keyNameValue;
                }
            }

            throw new ArgumentException($"The connection string is missing a required property: '{keyName}'");
        }
    }
}
