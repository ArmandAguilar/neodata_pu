import sys
import json
import requests
import logging
import subprocess
import pymssql
import pyodbc
import re
import pandas as pd


class neodataPu:

    def __init__(self,path_script='C:\\NEODATA2021\\BD\\Scripts\\',srv=''):
        self.srv = ''
        self.path_neodata_script = ''
        self.error = ''
        try:
            if srv != '':
                self.path_neodata_script = path_script
                self.srv = srv
            else:
                #GET THE INSTANSE SERVER np:\\.\pipe\LOCALDB#D0F9F005\tsql\query
                subprocess.check_output('sqllocaldb start mssqllocaldb',shell=True)
                r = subprocess.check_output('sqllocaldb info mssqllocaldb',shell=True)
                o = r.decode('utf-8')
                #strCon = str(o[251:-1]).replace('\r\r','').replace(': ','').replace('name','')
                strCon = str(o)
                patron = r'np:(.*?)(?=query)'
                resultado = re.search(patron,strCon)
                if resultado:
                    subcadena = resultado.group(1)
                    conStr = 'np:' + resultado.group(1) + 'query'

                    #SELF Vars
                    self.path_neodata_script = path_script
                    self.srv = conStr
                
        except Exception as err:
            self.error = str(err)
    
    def getListDb(self):
        """
        This def get a list of all DB
        @param:self
        @type:object

        return {name:'C://FOO.mdf'}
        """
        data = []
        try:
            #1 GET A LIST OF DB [Budgets] AND CREATE A DB.JSON
            cmdDB = (r'' + str(self.path_neodata_script) + 'SQLCMD -S "' + str(self.srv) + r'" -h -1 -r 0 -Q "set nocount on; select name from sys.databases for JSON AUTO" > bases.json')
            subprocess.check_output(cmdDB,shell=True)

            #2 READ THE DB A PRINT  LIST
            f = open('bases.json',)
            data = json.load(f)

        except Exception as err:
            logging.info(str(err))
        
        return data
    
    def getVersions(self,db):
        """
        This def get a list of all version of a budget
        @param:self
        @type:object

        @param:db
        @type:str

        return {version:'version 1'}
        """
        data = []
        try:
            #1 GET A LIST OF DB [Budgets Version]
            sql = "SELECT IdPresupuesto,Presupuesto FROM PuPresupuestos FOR JSON PATH, ROOT('Budgets')"

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            # 2 CONVERT TO JSON
            data = json.loads(db_query[0][0])

        except Exception as err:
            logging.error(str(err))
        
        return data
    
    def getPartidasWSB(self,db,idBudget):
        """
        This def get a node of parts
        @param:self
        @type:object

        @param:db
        @type:str

        @param:idBudget
        @type:str

        return {}
        """
        data = []
        try:
            #1 .- The List of WBS
            sql = "SELECT DISTINCT [IdPresupuestoPartida],[PartidaWBS],[IdPartidaPadre],[DescripcionPartidaLarga] "\
                " FROM [dbo].[PuPresupuestosPartidas] "\
                " WHERE IdPresupuesto = '{0}' "\
                " order by [PartidaWBS],[IdPartidaPadre]".format(idBudget)

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            # 2.- CREATE THE JSON
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            
            if len(list(db_query)) > 0:
                for item in db_query:
                    data.append({
                        'IdPresupuestoPartida':item[0],
                        'PartidaWBS':item[1],
                        'IdPartidaPadre':item[2],
                        'DescripcionPartidaLarga':item[3],
                    })
        except Exception as err:
            logging.error(str(err))
        return data
    
    def getPartidasWSBWithCost(self,db,idBudget):
        """
        This def get a node with cost
        @param:self
        @type:object

        @param:db
        @type:str

        @param:idBudget
        @type:str

        return {}
        """
        data = []
        try:
            #1 .- The List of WBS
            sql = "SELECT  "\
                    "Partidas.[IdPresupuestoPartida],"\
                    "Partidas.[PartidaWBS],"\
                    "Partidas.[IdPartidaPadre],"\
                    "Partidas.[DescripcionPartidaLarga],"\
                    "PartidasCosto.[Costo],"\
                    "PartidasCosto.[Precio],"\
                    "PartidasCosto.[CostoTotal],"\
                    "PartidasCosto.[PrecioTotal],"\
                    "PartidasCosto.[Costo1Nivel],"\
                    "PartidasCosto.[Precio1Nivel] "\
                    "FROM [dbo].[PuPresupuestosPartidas] Partidas,[dbo].[PuPresupuestosPartidasCostos] PartidasCosto "\
                    "WHERE "\
                    "Partidas.IdPresupuesto = '{0}' AND "\
                    "Partidas.IdPresupuestoPartida = PartidasCosto.IdPresupuestoPartida "\
                    "order by Partidas.[PartidaWBS],Partidas.[IdPartidaPadre]".format(idBudget)

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            # 2.- CREATE THE JSON
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            
            if len(list(db_query)) > 0:
                for item in db_query:
                    data.append({
                        "IdPresupuestoPartida":item[0],
                        "PartidaWBS":item[1],
                        "IdPartidaPadre":item[2],
                        "DescripcionPartidaLarga":item[3],
                        "Costo":float(item[4]),
                        "Precio":float(item[5]),
                        "CostoTotal":float(item[6]),
                        "PrecioTotal":float(item[7]),
                        "Costo1Nivel":float(item[8]),
                        "Precio1Nivel":float(item[9])
                    })
        except Exception as err:
            logging.error(str(err))
        return data
    
    
    def getBudgetItems(self,db,idBudget):
        """
        This def get all items of budget
        @param:self
        @type:object

        @param:idBudget
        @type:str

        return {}
        """
        data = []
        try:
            #1 GET A LIST OF DB [Budgets] by Id
            sql = "SELECT  [IdPresupuestoConcepto],[IdPresupuesto],[IdPresupuestoPartida],[IdExpIns],[IdEstimacionTipo],[IdAgrupador] "\
                    ",[Control],[Renglon],[Cantidad],[CantidadTotal],[Marca],[CodigoAuxiliar],[DescripcionAuxiliar],[IndirectoEspecial],"\
                    "[PorcentajeIndirectoEspecial],[RutaGenerador],[AEstimar],[YaEstimado],[Programado],[IdCentro],[IdContratista],[Bloqueado]"\
                    " FROM .[PuPresupuestosConceptos] WHERE [IdPresupuesto]='{0}'".format(idBudget)

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            if len(list(db_query)) > 0:
                for item in db_query:
                    data.append({
                    'IdPresupuestoConcepto':item[0],
                    'IdPresupuesto':item[1],
                    'IdPresupuestoPartida':item[2],
                    'IdExpIns':item[3],
                    'IdEstimacionTipo':item[4],
                    'IdAgrupador':item[5],
                    'Control':item[6],
                    'Renglon':item[7],
                    'Cantidad':float(item[8]),
                    'CantidadTotal':float(item[9]),
                    'Marca':item[10],
                    'CodigoAuxiliar':item[11],
                    'DescripcionAuxiliar':item[12],
                    'IndirectoEspecial':float(item[13]),
                    'PorcentajeIndirectoEspecial':float(item[14]),
                    'RutaGenerador':item[15],
                    'AEstimar':item[16],
                    'YaEstimado':item[17],
                    'Programado':item[18],
                    'IdCentro':item[19],
                    'IdContratista':item[20],
                    'Bloqueado':item[21],
                    })
            data = json.loads(data)
        except Exception as err:
            logging.error(str(err))
        return data

    def getBudgetReport(self,db,idBudget,idBudgetHeading):
        """
        This def create all concept like a Reports CFE theme
        @param:self
        @type:object

        @param:idBudget
        @type:int

        @param:idBudgetHeading
        @type:int

        return {}
        """
        data = []
        try:
            #1 .- [Budgets] - Report
            sql = "SELECT   "\
                   "Presupuesto.IdPresupuestoConcepto As Id,"\
                    "Presupuesto.Control As Crt,"\
                    "Partida.DescripcionPartidaLarga As Partida,"\
                    "Catalogo.Codigo As Codigo,"\
                    "(0) As 'C.Cliente',"\
                    "Catalogo.DescripcionLarga As Concepto,"\
                    "Unidades.Unidad As Unidad,"\
                    "Presupuesto.Cantidad As Cantidad,"\
                    "(0) As 'Volumen',"\
                    "(0) As 'Kg_IM',"\
                    "(0) As 'Kg',"\
                    "(0) As 'Piezas',"\
                    "'.' As 'Base Nivel',"\
                    "'.' As 'Cuerpo',"\
                    "'.' As 'Tipo',"\
                    "'.' As 'Autor',"\
                    "'' AS 'Observaciones',"\
                    "Precios.[Precio] AS 'Precio' "\
                    "FROM [dbo].[PuPresupuestosConceptos] Presupuesto,[dbo].[PuPresupuestosPartidas] Partida,"\
                    "[dbo].[PuPresupuestosConceptosPrecios] Precios,[dbo].[PuExpIns] ExpIns,[dbo].[PuCatalogo] Catalogo,"\
                    "[dbo].[PuUnidades] Unidades "\
                    "WHERE "\
                    "Presupuesto.IdPresupuesto = '{0}' AND "\
                    "Presupuesto.IdPresupuestoPartida = '{1}' AND "\
                    "Partida.IdPresupuesto = '{0}' AND "\
                    "Partida.IdPresupuestoPartida = '{1}' AND "\
                    "Presupuesto.IdPresupuestoConcepto = Precios.[IdPresupuestoConcepto] AND "\
                    "Presupuesto.IdExpIns = ExpIns.IdExpIns AND "\
                    "ExpIns.IdCodigo = Catalogo.IdCodigo AND "\
                    "Catalogo.IdUnidad = Unidades.IdUnidad AND Presupuesto.Cantidad > '0' "\
                    " Order By Crt desc".format(idBudget,idBudgetHeading)

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            if len(list(db_query)) > 0:
                for item in db_query:
                    concept = str(item[5]).replace('\t','')
                    concept = concept.replace('\n','')
                    data.append({
                        'Id':item[0],
                        'Crt':item[1],
                        'Partida':item[2],
                        'Codigo':item[3],
                        'C.Cliente':item[4],
                        'Concepto':concept,
                        'Unidad':item[6],
                        'Cantidad':float(item[7]),
                        'Volumen':item[8],
                        'Kg_IM':item[9],
                        'Kg':item[10],
                        'Piezas':item[11],
                        'Base Nivel':item[12],
                        'Cuerpo':item[13],
                        'Tipo':item[14],
                        'Autor':item[15],
                        'Observaciones':item[16],
                        'Precio':float(item[17])
                    })
        except Exception as err:
            logging.error(str(err))
        return data

    def getCatalogo(self,db):
        """
        This def get a list of catalogo
        @param:self
        @type:object

        @param:db
        @type:str

        return {version:'version 1'}
        """
        data = []
        try:
            #1.- GET THE CATALOG
            sql = "SELECT "\
	                "Catalogo.[IdCodigo], "\
                    "Catalogo.[Codigo], "\
                    "Catalogo.[DescripcionLarga] As 'Descripcion', "\
                    "Catalogo.[IdUnidad], "\
                    "Unidades.[Unidad], "\
                    "Unidades.Descripcion As 'Descripcion Unidad' "\
                "FROM [dbo].[PuCatalogo] Catalogo,[dbo].[PuUnidades] Unidades "\
                "WHERE Catalogo.IdUnidad = Unidades.IdUnidad"

            conn_str = (
                        r'DRIVER={ODBC Driver 17 for SQL Server};'
                        r'SERVER=' + self.srv + ';'
                        r'PORT=1433;'
                        r'DATABASE=' + db + ';'
                        r'trusted_connection=yes;')

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()
            if len(list(db_query)) > 0:
                for item in db_query:
                    data.append({
                        'IdCodigo':item[0],
                    'Codigo':item[1],
                    'Descripcion':item[2],
                    'IdUnidad':item[3],
                    'Unidad':item[4],
                    'Descripcion_Unidad':item[5],
                    })
        except Exception as err:
            logging.error(str(err))
        
        return data      
