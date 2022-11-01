using System.Data;

namespace Database_Access_Layer
{
    
    public interface IDbContext
    {
        public bool InsertUpdateDelete(string sql, List<DalDbParameter> dbParameters);
        public Task<bool> InsertUpdateDeleteAsync(string sql, List<DalDbParameter> dbParameters);
        public DataTable Select(string sql);
        public Task<DataTable> SelectAsync(string sql);
        public DataTable Select(string sql, List<DalDbParameter> dbParameters);
        public Task<DataTable> SelectAsync(string sql, List<DalDbParameter> dbParameters);
        public DataTable ExecuteProcedure(string storedProcedureName);
        public Task<DataTable> ExecuteProcedureAsync(string storedProcedureName);
        public DataTable ExecuteProcedure(string storedProcedureName, List<DalDbParameter> dbParameters);
        public Task<DataTable> ExecuteProcedureAsync(string storedProcedureName, List<DalDbParameter> dbParameters);
        public void BackupDatabase(string databaseName, string backupFolderPath);
        public Task BackupDatabaseAsync(string databaseName, string backupFolderPath);
    }
}
