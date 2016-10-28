using System.Data;
using System.Data.Common;
using NUnit.Framework;

namespace AsyncDataAdapter.Tests
{
    [TestFixture]
    public class HelpersTest
    {
        [Test]
        public void AdapterInitIntShouldWork()
        {
            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);

            Assert.DoesNotThrow(() => e.AdapterInit(10));
        }

        [Test]
        public void AdapterInitDataRowArrayShouldWork()
        {
            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);

            Assert.DoesNotThrow(() => e.AdapterInit(new DataRow[] {}));
        }

        [Test]
        public void GetRowsShouldWork()
        {
            var t = new DataTable();

            var dataRow = t.NewRow();

            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);
            e.AdapterInit(new []{dataRow});

            var rows = e.GetRows();

            Assert.AreEqual(1, rows.Length);
            Assert.AreEqual(dataRow, rows[0]);
        }

        [Test]
        public void GetRowsForSingleRowShouldWork()
        {
            var t = new DataTable();

            var dataRow = t.NewRow();

            var e = new RowUpdatedEventArgs(null, null, StatementType.Select, null);
            e.AdapterInit(new []{dataRow});

            var row = e.GetRows(0);

            Assert.AreEqual(dataRow, row);
        }

        [Test]
        public void EnsureAdditionalCapacityShouldWork()
        {
            var c = new DataTable().Columns;
            
            Assert.DoesNotThrow(() => c.EnsureAdditionalCapacity(10));
        }

        
    }
}