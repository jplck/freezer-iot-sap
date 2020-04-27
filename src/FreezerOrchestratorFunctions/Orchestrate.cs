using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Collections.Generic;
using System.Text;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Mvc;

namespace DemonstratorFuncs
{
    public class StreamAnalyticsPayload
    {
        [JsonProperty("allevents")]
        public List<StreamAnalyticsPayloadValue> Values { get; set; }
    }

    public class StreamAnalyticsPayloadValue
    {

        [JsonProperty("temperature")]
        public double Temperature { get; set; }

        [JsonProperty("ambienttemperature")]
        public double AmbientTemperature { get; set; }

        [JsonProperty("timeCreated")]
        public DateTime TimeCreated { get; set; }

        [JsonProperty("ConnectionDeviceId")]
        public string ConnectionDeviceId { get; set; }

        [JsonProperty("ConnectionDeviceGenerationId")]
        public string ConnectionDeviceGenerationId { get; set; }

    }
    public class AKSPayload
    {
        [JsonProperty("deviceId")]
        public string DeviceId { get; set; }

        [JsonProperty("ts")]
        public DateTime TimeStamp;

        [JsonProperty("classificiation")]
        public bool Classificiation;
    }

    public static class Orchestrate
    {
        [FunctionName("ModelValidator")]
        public static async Task<IActionResult> ModelValidatorTriggerAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "validate")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient client,
            ILogger log)
        {
            StreamReader reader = new StreamReader(req.Body);
            string body = reader.ReadToEnd();

            StreamAnalyticsPayload convertedPayload;

            try {
                convertedPayload = JsonConvert.DeserializeObject<StreamAnalyticsPayload>(body);
            }
            catch (JsonException jsonException) 
            {
                log.LogError(jsonException.Message);
                return await Task.FromResult(new BadRequestResult());
            }

            await client.StartNewAsync("ModelOrchestrator", convertedPayload);
            return await Task.FromResult(new AcceptedResult("validate", $"Model has been called."));
        }

        [FunctionName("ModelOrchestrator")]
        public static async Task ModelOrchestratorAsync(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log)
        {
            var input = context.GetInput<StreamAnalyticsPayload>();

            var retryOptions = new RetryOptions(new TimeSpan(0, 0, 15), 5);
            var modelResults = await context.CallActivityWithRetryAsync<AKSPayload>("CallModel", retryOptions, input);

            var validateModelResults = await context.CallActivityAsync<bool>("ValidateClassification", modelResults);
        }

        [FunctionName("CallModel")]
        public static async Task<AKSPayload> CallModelAsync([ActivityTrigger] StreamAnalyticsPayload payloadFromStreamAnalytics, ILogger log)
        {
            var endpointUrl = Environment.GetEnvironmentVariable("MLENDPOINT");
            var jsonPayload = JsonConvert.SerializeObject(payloadFromStreamAnalytics);

            if (string.IsNullOrWhiteSpace(endpointUrl)) 
            {
                throw new FunctionFailedException("Endpoint should not be null. Please set a endpoint in environment.");
            }

            try
            {
                var client = new HttpClient();
                var data = new StringContent(jsonPayload, Encoding.UTF8);
                data.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                var response = await client.PostAsync(endpointUrl, data);

                string result = response.Content.ReadAsStringAsync().Result;
                var deSerializedResult = JsonConvert.DeserializeObject<AKSPayload>(jsonPayload);
                 return deSerializedResult;
            }
            catch(HttpRequestException httpEx)
            {
                throw new FunctionFailedException($"Unable to send request to MLEndpoint. ({httpEx.Message})");
            }
            catch(JsonException jsonException)
            {
                throw new FunctionFailedException($"Unable to deserialize data. ({jsonException.Message})");
            }
        }

        [FunctionName("ValidateClassification")]
        public static bool ValidateClassification([ActivityTrigger] AKSPayload classificaiton, ILogger log)
        {
            //validate classifcation and send notification to phone or sap.
            return true;
        }

    }
}