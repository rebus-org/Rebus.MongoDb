# Changelog

## 2.0.0-a1
* Test release

## 2.0.0-b01
* Test release

## 2.0.0
* Release 2.0.0

## 3.0.0
* Update to Rebus 3

## 4.0.0
* Update to Rebus 4
* Add .NET Core support
* Add data bus storage based on GridFS

## 5.0.0
* Update to Rebus 5.0.0 because it has a more BSON serialization-friendly `IdempotencyData`
* Add .NET Standard 2.0 and .NET 4.6 as targets

## 5.1.0
* Fix uniqueness of correlation properties
* Fix customization of serialization of saga data's ID - thanks [cgehrmann]

## 6.0.0
* Update Rebus dep to 6
* Add data bus management implementation

## 6.0.3
* Add automatic guard against accidentally using a customized BSON serializer that interferes with Rebus. Unfortunately this can only be done at runtime, resulting in an `BsonSchemaValidationException` the first time a problematic saga data is encountered
* Fix bug that would result in accidentally creating an index on the 'Id' field, which is called '_id' in MongoDB documents and is already indexed

[cgehrmann]: https://github.com/cgehrmann
