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