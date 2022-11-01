using System.Data;

namespace Database_Access_Layer
{
    public class DalDbParameter
    {
        public string Name { get; set; }
        public object Value { get; set; }
        public SqlDbType? Type { get; set; }

        public DalDbParameter(string name, object value, SqlDbType? type)
        {
            Name = name;
            Value = value;
            Type = type;
        }
    }
}
