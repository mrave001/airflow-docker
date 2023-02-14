import os
import jaydebeapi
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

class TeradataHook(JdbcHook):
    """
    A clone of the JdbcHook except without the need for users to configure the driver name and
    location. Although the connection type doesn't have an affect on anything past the UI it
    would make the most sense for it to be a Jdbc connection.
    """

    @staticmethod
    def get_path_prefix():
        return os.path.join(os.path.dirname(__file__), 'resources/')

    def get_conn(self):
        conn = self.get_connection(self.jdbc_conn_id)
        host = conn.host
        login = conn.login
        psw = conn.password

        prefix = self.get_path_prefix()
        tdgs_config_loc = os.path.join(prefix, 'tdgssconfig.jar')
        teradata_loc = os.path.join(prefix, 'terajdbc4.jar')
        jdbc_driver_name = 'com.teradata.jdbc.TeraDriver'

        # print(f"Host: {str(host)}")
        # print(f"Teradata Loc: {teradata_loc}")

        return jaydebeapi.connect(jclassname=jdbc_driver_name,
                                  url=str(host),
                                  driver_args=[str(login), str(psw)],
                                  jars=[teradata_loc]) #, tdgs_config_loc])
