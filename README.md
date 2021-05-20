# Rebus.MongoDb

[![install from nuget](https://img.shields.io/nuget/v/Rebus.MongoDb.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.MongoDb)

Provides MongoDB persistence implementations for [Rebus](https://github.com/rebus-org/Rebus) for

* sagas
* subscriptions
* timeouts

# Unit Tests

To run unit test please provide a mongo instance to run test and set the connection string ino REBUS_MONGODB environment variable.

# Local nuget pack

To manually create a nuget package specifying manually the version you can use this commandline.

```
dotnet pack Rebus.MongoDb\Rebus.MongoDb.csproj -o \nuget -c release /p:PackageVersion=6.0.1006 /p:AssemblyVersion=6.0.0 /p:FileVersion=6.0.0 /p:InformationalVersion=6.0.0-sha
```

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---


