using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using NUnit.Framework;

namespace AsyncDataAdapter.Tests
{
    [TestFixture]
    public class SqlDataAdapterTest
    {
        private const string ConnectionString = @"server=.\sqlexpress;database=TestDb;Trusted_Connection=Yes";

        [Test]
        public async Task FillAsyncDataTable()
        {
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

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public void FillDataTable()
        {
            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (var c = conn.CreateCommand())
                {
                    c.CommandText = "GetFast";
                    c.CommandType = CommandType.StoredProcedure;
                    c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                    var a = new System.Data.SqlClient.SqlDataAdapter(c);
                    var dt = new DataTable();
                    var r = a.Fill(dt);

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public async Task FillAsyncDataSet()
        {
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

                    Assert.AreEqual(1, ds.Tables.Count);
                    var dt = ds.Tables[0];

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        [Test]
        public void FillDataSet()
        {
            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (var c = conn.CreateCommand())
                {
                    c.CommandText = "GetFast";
                    c.CommandType = CommandType.StoredProcedure;
                    c.Parameters.Add("@Number", SqlDbType.Int).Value = 100000;

                    var a = new System.Data.SqlClient.SqlDataAdapter(c);
                    var ds = new DataSet();
                    var r = a.Fill(ds);

                    Assert.AreEqual(1, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(900000, r);
                    Assert.AreEqual(900000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }
        [Test]
        public async Task FillAsyncDataSetMulti()
        {
            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                await conn.OpenAsync();

                using (var c = conn.CreateCommand())
                {
                    c.CommandText = "GetMulti";
                    c.CommandTimeout = 600000;
                    c.CommandType = CommandType.StoredProcedure;
                    c.Parameters.Add("@Number1", SqlDbType.Int).Value = 100000;
                    c.Parameters.Add("@Number2", SqlDbType.Int).Value = 300000;
                    c.Parameters.Add("@Number3", SqlDbType.Int).Value = 500000;

                    var a = new SqlDataAdapter(c);
                    var ds = new DataSet();
                    var r = await a.FillAsync(ds);

                    Assert.AreEqual(8, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(50000, r);
                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[6];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[7];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }


        [Test]
        public void FillDataSetMulti()
        {
            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();

                using (var c = conn.CreateCommand())
                {
                    c.CommandText = "GetMulti";
                    c.CommandTimeout = 600000;
                    c.CommandType = CommandType.StoredProcedure;
                    c.Parameters.Add("@Number1", SqlDbType.Int).Value = 100000;
                    c.Parameters.Add("@Number2", SqlDbType.Int).Value = 300000;
                    c.Parameters.Add("@Number3", SqlDbType.Int).Value = 500000;

                    var a = new System.Data.SqlClient.SqlDataAdapter(c);
                    var ds = new DataSet();
                    var r = a.Fill(ds);

                    Assert.AreEqual(8, ds.Tables.Count);

                    var dt = ds.Tables[0];

                    Assert.AreEqual(50000, r);
                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[6];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);

                    dt = ds.Tables[7];

                    Assert.AreEqual(50000, dt.Rows.Count);

                    AssertDataTableContent(dt);
                }
            }
        }

        private void AssertDataTableContent(DataTable dt)
        {
            int i = 1;

            do
            {
                var previousRow = dt.Rows[i - 1];

                var flt = (double)previousRow["FltVal"];
                var dec = (decimal)previousRow["DecVal"];
                var st = (DateTime)previousRow["StartDate"];
                var txt = (string)previousRow["Txt"];

                flt += .1f;
                dec += (decimal).1;

                var currentRow = dt.Rows[i];

                var aflt = (double)currentRow["FltVal"];
                var adec = (decimal)currentRow["DecVal"];
                var ast = (DateTime)currentRow["StartDate"];
                var atxt = (string)currentRow["Txt"];

                Assert.AreEqual(flt, aflt, .01);
                Assert.AreEqual(dec, adec);
                Assert.AreEqual(st, ast);
                Assert.AreEqual(txt, atxt);
                i++;
            } while (i < dt.Rows.Count);
        }


    }
}