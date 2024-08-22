import pyodbc
import os
import pandas as pd
import logging
import numpy as np
from logger import CustomFormatter

from shapely.geometry import MultiPolygon
from shapely.wkt import dumps, loads
from shapely.geometry import Point

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


# USE CVM ACCSESS
# GRANT SELECT , UPDATE, INSERT, ALTER ON temp.clv TO "VIVACELL\hearakelyan";


class SQLHandler:

    def __init__(
        self, SERVER: str = "VCBI", DATABASE: str = "A_SRV", SCHEMA: str = "dbo"
    ) -> None:
        server = SERVER
        self.database = DATABASE
        self.schema = SCHEMA
        self.table_name = None
        trusted = "yes"
        connection_string = f"DRIVER=SQL SERVER; SERVER={server}; Database={self.database}; TRUSTED_CONNECTION={trusted};"
        logger.info(f"connection string: {connection_string}")
        self.cnxn = pyodbc.connect(connection_string)
        self.cursor = self.cnxn.cursor()

    def __repr__(self) -> str:
        return f"{self.table_referance}"

    
    def GetTableReferance(self):
        if self.schema is None:
            self.table_referance = self.table_name
        else:
            self.table_referance = f"{self.database}.{self.schema}.{self.table_name}"
        logger.info(f"The table referance: {self.table_referance}")

    def CreateTable(self, columns: tuple):
        # if SCH is None:
        
        query = f"""  
                CREATE TABLE  {self.table_referance}
                    ({columns})                    
                    ;
            """
        logger.debug(query)
        self.cursor.execute(query)

        logger.info(f"the {self.table_referance} is created")

    def Commit(self):
        logger.info("commiting query")
        self.cursor.commit()

    # def select_top_n_values(self,table_name:str,schema:='dbo',N:int=100):
    #     query=f"""
    #             SELECT TOP {N}
    #                 *
    #             FROM {self.db}
    #         """

    def Mapper(self,data:pd.DataFrame):
        mappings={
        'object':'VARCHAR(MAX)',
        'datetime64[ns]': 'DATE',
        'int64':'INT',
        'float64':'FLOAT'
        }

        l=[]
        for i, value in enumerate(data.columns):
            pandas_data_type=mappings[data[value].dtype.name]
            l.append(f'{value} {pandas_data_type}')
        
        columns=', \n'.join(l)
        logger.info(columns)
        return columns
    
    def TruncateTable(self):
        query = f"""TRUNCATE TABLE {self.table_referance}"""
        self.cursor.execute(query)
        logger.info(f"the {self.table_referance} is truncated")

    def DropTable(self):
        query = f"""DROP TABLE IF EXISTS {self.table_referance}"""
        self.cursor.execute(query)
        logger.info(query)

    def DropRows(self, condition: str):
        """_summary_
        >> example DATE_ID>''
        Args:
            condition (str): _description_
        """
        query = f"""DELETE FROM {self.table_referance}
                    WHERE {condition}
                """
        self.cursor.execute(query)
        logger.info(query)

    def Close(self):
        # try:
        self.cursor.close()
        self.cnxn.close()
        logger.info("The Connection is successfully closed")

    def GetColumns(self, default: bool = True) -> list:
        if default:
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}' AND COLUMN_DEFAULT IS NULL"
        else:
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}'"
        self.cursor.execute(query)
        columns = [column[0] for column in self.cursor.fetchall()]
        logger.info(f"the list of columns in {self.table_referance}")
        return columns

    def ExecuteSqlProcedure(self, procedure_name: str, arguments: dict):
        arguments = ", ".join(arguments.values())

        logger.info(
            f"""RUNNING EXEC {self.database}.{self.schema}.{procedure_name} {arguments} """
        )
        self.cursor.execute(
            f"""EXEC {self.database}.{self.schema}.{procedure_name} {arguments}"""
        )
        self.cursor.commit()
        logger.info(
            f"""FINISHED EXEC {self.database}.{self.schema}.{procedure_name} {arguments}"""
        )
        import re

        text_to_remove = "[Microsoft][ODBC SQL Server Driver][SQL Server]"
        try:
            for i in self.cursor.messages:
                message = i[len(i) - 1].replace(text_to_remove, "")
                logger.info(f"SQL MESSAGE: {message}")
        except:
            pass
    
    def GetListofProcedures(self)->list:
        query="""
                SELECT 
                DISTINCT 
                SPECIFIC_NAME Procedures
            FROM INFORMATION_SCHEMA.PARAMETERS
        """
        logger.info('sadf')
        self.cursor.execute(query)
        Procedures = [Procedure[0] for Procedure in self.cursor.fetchall()]
        logger.info(f"the list of procedures: {Procedures}")
        return Procedures

    def GetListofProcedureParams(self,Procedure_Name:str)->pd.DataFrame:
        query=f"""
            SELECT 
                SPECIFIC_NAME AS ProcedureName,
                PARAMETER_NAME AS ParameterName,
                ORDINAL_POSITION AS Position,
                --PARAMETER_MODE AS Mode,
                DATA_TYPE AS DataType
                --CHARACTER_MAXIMUM_LENGTH AS MaxLength
            FROM 
                INFORMATION_SCHEMA.PARAMETERS
                where PARAMETER_MODE='IN' AND SPECIFIC_NAME='{Procedure_Name}'

            ORDER BY 
                ORDINAL_POSITION;
        """

        data=self.FromSqlToPandas(query)
        logger.info(f'the list of Params for {Procedure_Name} are TODO')
        return data


    def GrantAccess(self, access_type: list, users: list):

        query = f"""  
                    GRANT {', '.join(access_type)} ON {self.table_referance} TO {users};
                """
        logger.info(query)
        self.cursor.execute()
        logger.info(
            f"{access_type} accesses are provided to {users} for {self.table_referance} table"
        )

    def FromPandasToSql(self, df:pd.DataFrame) -> str:
        """_summary_

        Args:
            df (pd.DataFrame): _description_

        Returns:
            str: _description_
        """
        df = df.replace(np.nan, None)  # for handling NULLs
        df.rename(columns=lambda x: x.lower(), inplace=True)

        columns = df.columns
        logger.info(f"BEFORE the column intersection: {columns}")
        sql_column_names = [i.lower() for i in self.GetColumns(default=True)]
        logger.info(
            f"the list of columns in {self.table_referance}: {sql_column_names}"
        )
        columns = list(set(columns) & set(sql_column_names))
        logger.info(f"AFTER the column intersection: {columns}")
        ncolumns = list(len(columns) * "?")
        data_to_insert = df.loc[:, columns]

        values = [tuple(i) for i in data_to_insert.values]
        logger.info(
            f"the shape of the table which is going to be imported {data_to_insert.shape}"
        )
        if "geometry" in columns:
            df["geometry"] = df["geometry"].apply(lambda geom: dumps(geom))
            ncolumns[columns.index("geometry")] = "geography::STGeomFromText(?, 4326)"

        # if "geography" in columns:
        #     df["geography"] = df["geography"].apply(lambda geom: dumps(geom))
        #     ncolumns[columns.index("geometry")] = "geography::STGeomFromText(?, 4326)"

        if len(columns) > 1:
            cols, params = ", ".join(columns), ", ".join(ncolumns)
        else:
            cols, params = columns[0], ncolumns[0]

        logger.info(f"insert structure: colnames: {cols} params: {params}")
        logger.info(values[0])
        query = f"""INSERT INTO  {self.table_referance} ({cols}) VALUES ({params});"""

        logger.info(f"QUERY: {query}")

        self.cursor.executemany(query, values)

        logger.warning("the data is loaded")


    def FromSqlToPandas(self,query:str,date_columns:list=[],dtypes:dict={})->pd.DataFrame:
        nqueries=query.split('-----')
        if len(nqueries)>1:
            logger.info(f'{len(nqueries)} Queries has been detected')
            cursor=self.cnxn.cursor()
            for i in nqueries[:-1]: # selecting all the elements except the last one
                cursor.execute(i)
                logger.info(f'EXEECUTING: {i}')
            cursor.commit()
            data=pd.read_sql_query(nqueries[-1],self.cnxn, parse_dates=date_columns, dtype=dtypes)
            cursor.close()
        else:
            data = pd.read_sql_query(query,self.cnxn, parse_dates=date_columns, dtype=dtypes)
        
        self.cnxn.close()
        logger.info(f'the shape of the data: {data.shape}')
        return data
       
    
    def FromSqlToCSV(self,query:str,date_columns:list=[],dtypes:dict={},filename:str=None)->pd.DataFrame.shape:
        data = self.FromSqlToPandas()
        data.to_csv(filename,index=False)
        print(data.shape)
        return data.shape
