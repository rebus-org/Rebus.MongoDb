using MongoDB.Bson.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.MongoDb.Tests.Helpers
{
    public class MongoDbTestHelper
    {
        public static class BsonClassMapHelper
        {
            public static void Unregister<T>()
            {
                var classType = typeof(T);
                GetClassMap().Remove(classType);
            }

            static Dictionary<Type, BsonClassMap> GetClassMap()
            {
                var cm = BsonClassMap.GetRegisteredClassMaps().First();
                var fi = typeof(BsonClassMap).GetField("__classMaps", BindingFlags.Static | BindingFlags.NonPublic);
                var classMaps = (Dictionary<Type, BsonClassMap>)fi.GetValue(cm);
                return classMaps;
            }

            public static void Clear()
            {
                GetClassMap().Clear();
            }
        }
    }
}
