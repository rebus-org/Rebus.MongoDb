# Rebus.MongoDb

[![install from nuget](https://img.shields.io/nuget/v/Rebus.MongoDb.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.MongoDb)

Provides MongoDB persistence implementations for [Rebus](https://github.com/rebus-org/Rebus) for

* sagas
* subscriptions
* timeouts
 
Provides transport MongoDb implementations for [Rebus](https://github.com/rebus-org/Rebus) 

# Saga Data

You must ensure to map `GuidRepresentation` for classes inheriting from `ISagaData` as this is required for any `Guid`
field in `MongoDB.Driver` from 3.x and beyond.

Remember, if you have existing saga data from prior to `MongoDB.Driver` 3.x, this will use 
`GuidRepresentation.CSharpLegacy` so you must take care to ensure you map it appropriately.

## Inheriting from `ISagaData`

If your saga data class inherits from the `ISagaData` interface, you can either use attributes:
```csharp
public class MySagaData : ISagaData
{
    [BsonGuidRepresentation(GuidRepresentation.Standard)]
    public Guid Id { get; set; } 
    public int Revision { get; set; }
}
```

Or you can create a `BsonClassMap`:
```csharp
BsonClassMap.RegisterClassMap<MySagaData>(map =>
{
    map.MapIdMember(obj => obj.Id).SetSerializer(new GuidSerializer(GuidRepresentation.Standard));
    map.MapMember(obj => obj.Revision);
});
```

## Inheriting from `SagaData`

If you inherit from `SagaData`, you can just create a `BsonClassMap` which will be applied to all classes inheriting
from the base class:
```csharp
BsonClassMap.RegisterClassMap<SagaData>(map =>
{
    map.MapIdMember(obj => obj.Id).SetSerializer(new GuidSerializer(GuidRepresentation.Standard));
    map.MapMember(obj => obj.Revision);
});
```

## Global serializer

A third option, if you prefer to configure a representation globally is:
```csharp
BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));
```

Remember, this will apply it globally for any code using `MongoDB.Driver`, including your own. So this is only an option
if all your code uses same representation across all collections.


# Unit Tests

To run unit test please provide a mongo instance to run test and set the connection string ino REBUS_MONGODB environment variable.

# Local nuget pack

To manually create a nuget package specifying manually the version you can use this commandline.

```
dotnet pack Rebus.MongoDb\Rebus.MongoDb.csproj -o c:\target_directory -c release /p:PackageVersion=6.0.1011 /p:AssemblyVersion=6.0.0 /p:FileVersion=6.0.0 /p:InformationalVersion=6.0.0-__SHA_OF_COMMIT__

//then you need to push
dotnet nuget push .\Rebus.MongoDb.6.0.1011.nupkg -s https://pkgs.dev.azure.com/xxxxx/_packaging/__packageName__/nuget/v3/index.json --api-key az
```

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---