using Newtonsoft.Json;

namespace Rebus.MongoDb.Tests
{
    public static class JsonExtensions
    {
        public static string ToPrettyJson(this object obj) => JsonConvert.SerializeObject(obj, Formatting.Indented);
    }
}