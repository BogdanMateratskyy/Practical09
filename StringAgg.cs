using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Server;

namespace Practical09
{
    public class Tuple
    {
        public string Item1;
        public string Item2;

        public Tuple(string value, string delimiter)
        {
            this.Item1 = value;
            this.Item2 = delimiter;
        }
    }

    /// <summary>Represent custom aggregator for string summing in SQL server.</summary>
    [Serializable]
    [SqlUserDefinedAggregate(Format.UserDefined,
        IsInvariantToDuplicates = false,
        IsInvariantToNulls = true,
        IsInvariantToOrder = false,
        IsNullIfEmpty = true,
        MaxByteSize = -1,
        Name = "StrSumEx")]
    public struct SqlStringSummator : IBinarySerialize, INullable
    {
        /// <summary>Constant value for initialization creating of internal state storage - elements counter.</summary>
        private const int _StartAllocateSize = 102400;

        /// <summary>Internal storage for characters count in resulted string.</summary>
        private int _length;

        /// <summary>Internal state storage.</summary>
        private List<Tuple> _state;

        #region IBinarySerialize Members

        /// <summary>Restore state of this structure from binary stream.</summary>
        /// <param name="reader">Binary reader for getting data.</param>
        public void Read(BinaryReader reader)
        {
            this._length = reader.ReadInt32();
            int counter = reader.ReadInt32();
            this._state =
                new List<Tuple>(2 * counter - _StartAllocateSize < 0 ? _StartAllocateSize : 2 * counter);
            for (int i = 0; i < counter; i++)
                this._state.Add(new Tuple(reader.ReadString(), reader.ReadString()));
        }

        /// <summary>Write state of this structure to stream.</summary>
        /// <param name="writer">Binary writer for writing data.</param>
        public void Write(BinaryWriter writer)
        {
            writer.Write(this._length);
            writer.Write(this._state.Count);
            foreach (Tuple tuple in this._state)
            {
                writer.Write(tuple.Item1);
                writer.Write(tuple.Item2);
            }
        }

        #endregion

        #region INullable Members

        /// <summary>Check if this structure results into null db object.</summary>
        public Boolean IsNull
        {
            get { return this._state.Count == 0; }
        }

        #endregion

        /// <summary>New structure initialization. It is always called before using.</summary>
        public void Init()
        {
            this._length = 0;
            this._state = new List<Tuple>(_StartAllocateSize);
        }

        /// <summary>Accumulate input values into internal state storage.</summary>
        /// <param name="value">String value for summing.</param>
        /// <param name="delimiter">Option delimiter between accumulated strings.</param>
        public void Accumulate(
            [SqlFacet(IsFixedLength = false, IsNullable = true, MaxSize = -1)] SqlString value,
            [SqlFacet(IsFixedLength = false, IsNullable = true, MaxSize = -1)] SqlString delimiter)
        {
            if (!value.IsNull)
            {
                Tuple tuple = new Tuple(value.Value,
                    delimiter.IsNull ? "" : delimiter.Value);
                this._length += tuple.Item1.Length + tuple.Item2.Length;
                this._state.Add(tuple);
            }
        }

        /// <summary>Merge this structure with results for other summing groups.</summary>
        /// <param name="group">Structure for merging.</param>
        public void Merge(SqlStringSummator group)
        {
            this._length += group._length;
            this._state.AddRange(group._state);
        }

        /// <summary>Finish summing.</summary>
        /// <returns>SQL string with summing aggregations result.</returns>
        [return: SqlFacet(IsFixedLength = false, IsNullable = true, MaxSize = -1)]
        public SqlString Terminate()
        {
            if (this.IsNull)
                return SqlString.Null;

            StringBuilder builder = new StringBuilder(this._length);
            for (int i = 0; i < this._state.Count - 1; i++)
                builder.Append(this._state[i].Item1).Append(this._state[i].Item2);
            if (this._state.Count > 0)
                builder.Append(this._state[this._state.Count - 1].Item1);
            return new SqlString(builder.ToString());
        }
    }
}
