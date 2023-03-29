using System;
using Rebus.Time;

namespace Rebus.MongoDb.Tests.DataBus;

class FakeRebusTime : IRebusTime
{
    DateTimeOffset? _fakeTime;

    public DateTimeOffset Now => _fakeTime ?? DateTimeOffset.Now;

    public void SetTime(DateTimeOffset fakeTime)
    {
        _fakeTime = fakeTime;
    }
}