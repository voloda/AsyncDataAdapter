[![Build status](https://ci.appveyor.com/api/projects/status/bw8gl0fp62vmia15/branch/master?svg=true)](https://ci.appveyor.com/project/voloda/asyncdataadapter/branch/master)

# AsyncDataAdapter

Implementation of asynchronous methods on SqlDataAdapter (support for async/await).

The implementation is based on source code provided by Microsoft on GitHub.

## Info

* Branch ```DotNetSource``` contains original Microsoft .NET sources to simplify resynchronization. Comments contains original sources.
* Only **read** operations are now supported. Feel free to contribute with the rest.

## Nuget package

[https://www.nuget.org/packages/AsyncDataAdapter/](https://www.nuget.org/packages/AsyncDataAdapter/)

## Sample usage for DataTable

```csharp
using (var conn = new SqlConnection())
{
    conn.ConnectionString = ConnectionString;
    await conn.OpenAsync();

    using (var c = conn.CreateCommand())
    {
        c.CommandText = "GetFast";
        c.CommandType = CommandType.StoredProcedure;
        c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        var a = new SqlDataAdapter(c);
        var dt = new DataTable();
        var r = await a.FillAsync(dt);
    }
}
```

## Sample usage for DataSet

```csharp
using (var conn = new SqlConnection())
{
    conn.ConnectionString = ConnectionString;
    await conn.OpenAsync();

    using (var c = conn.CreateCommand())
    {
        c.CommandText = "GetFast";
        c.CommandType = CommandType.StoredProcedure;
        c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

        var a = new SqlDataAdapter(c);
        var ds = new DataSet();
        var r = await a.FillAsync(ds);
    }
}
```
