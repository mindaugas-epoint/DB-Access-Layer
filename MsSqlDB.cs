
using System.Data;
using Microsoft.Data.SqlClient;

namespace Database_Access_Layer
{
    public class MsSqlDB : IDbContext
    {
        private string _ConnectionString;
        private bool _WinOS;

        public MsSqlDB(string connectionString, bool winOS = true)
        {
            _ConnectionString = connectionString;
            _WinOS = winOS;
        }

        public DataTable ExecuteProcedure(string storedProcedureName, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                    }
                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public async Task<DataTable> ExecuteProcedureAsync(string storedProcedureName, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                await sqlConnection.OpenAsync();

                using (SqlCommand sqlCommand = new SqlCommand(storedProcedureName, sqlConnection))
                { 
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                    }
                    SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public DataTable ExecuteProcedure(string storedProcedureName)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    SqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public async Task<DataTable> ExecuteProcedureAsync(string storedProcedureName)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                await sqlConnection.OpenAsync();

                using (SqlCommand sqlCommand = new SqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    SqlDataReader sqlDataReader = await sqlCommand.ExecuteReaderAsync();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public bool InsertUpdateDelete(string sql, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                    }

                    return sqlCommand.ExecuteNonQuery() > 0;
                }
            }
        }

        public async Task<bool> InsertUpdateDeleteAsync(string sql, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                    }

                    return await sqlCommand.ExecuteNonQueryAsync() > 0;
                }
            }
        }

        public DataTable Select(string sql)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            sqlDataAdapter.Fill(dt);
                            return dt;
                        }
                    }

                }
            }
        }

        public async Task<DataTable> SelectAsync(string sql)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            await Task.Run(() => sqlDataAdapter.Fill(dt));
                            return dt;
                        }
                    }

                }
            }
        }

        public DataTable Select(string sql, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    if (parameters != null && parameters.Count > 0)
                    {
                        foreach (var parameter in parameters)
                        {
                            sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                        }

                    }

                    using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            sqlDataAdapter.Fill(dt);
                            return dt;
                        }
                    }

                }
            }
        }

        public async Task<DataTable> SelectAsync(string sql, List<DalDbParameter> parameters)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConnection))
                {
                    sqlCommand.CommandType = System.Data.CommandType.Text;

                    if (parameters != null && parameters.Count > 0)
                    {
                            foreach (var parameter in parameters)
                            {
                                sqlCommand.Parameters.Add(new SqlParameter() { ParameterName = parameter.Name, Value = parameter.Value, SqlDbType = (SqlDbType)parameter.Type });
                            }

                        }

                    using (SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            await Task.Run(() => sqlDataAdapter.Fill(dt));
                            return dt;
                        }
                    }

                }
            }
        }

    }
}

