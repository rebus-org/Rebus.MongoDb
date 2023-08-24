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

## 6.0.4
* Fix bug that would result in indexes being initialized over and over

## 6.0.5
* Make automatic index creation on saga properties optional

## 7.0.0-b3
* Only target .NET Standard 2.0
* Update MongoDB driver to 2.13.1 (fix bug that would cause Rebus.MongoDb to malfunction on .NET 5)
* Updated MongoDB driver to 2.15.1 
* Restored multi targeted build (Still need to be used in Net48 project)
* Added MongoDb Transport - thanks [AGiorgetti] and [alkampfergit]

## 8.0.0-alpha04
* Remove MongoDB transport again, because it could lose messages and thus did not qualify as a proper Rebus transport
* Update MongoDB driver to 2.19.2
* Update to Rebus 8
* Bring back MongoDB transport - thanks [AlesDo]
* Make it possible to opt out of index creation when using MongoDB as the transport


[AGiorgetti]: https://github.com/AGiorgetti
[AlesDo]: https://github.com/AlesDo
[alkampfergit]: https://github.com/alkampfergit
[cgehrmann]: https://github.com/cgehrmann
