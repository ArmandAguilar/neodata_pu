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
            sql = "select name from sys.databases where name not in ('master','tempdb','model','msdb')"
            db = 'master'
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
            if len(list(db_query)) > 0:
                for item in db_query:
                    data.append({'name':item[0]})
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
            sql = "SELECT DISTINCT [IdPresupuestoPartida],[PartidaWBS],[IdPartidaPadre],[DescripcionPartidaLarga],[Cantidad] "\
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
                        'Cantidad':item[4]
                    })

            conn.close()
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
                    "PartidasCosto.[Precio1Nivel],"\
                    "Partidas.[Cantidad] "\
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
            conn.close()
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
                        'Id':'0',
                        'Crt':item[0],
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

    def getBudgeBodySimpleIndet(self,db,idBudget):

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
        wbs = []
        cuerpos = []
        cuerpo = ''
        partida = ''
        nivel = ''
        idBudgetHeading = 0
        try:
            #0 .- GET THE MAIN NODE WSBP
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
                    "PartidasCosto.[Precio1Nivel],"\
                    "Partidas.[Cantidad] "\
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
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()

            if len(list(db_query)) > 0:
                for item in db_query:
                    wbs.append({
                        "IdPresupuestoPartida":item[0],
                        "PartidaWBS":item[1],
                        "IdPartidaPadre":item[2],
                        "DescripcionPartidaLarga":item[3],
                        "Costo":float(item[4]),
                        "Precio":float(item[5]),
                        "CostoTotal":float(item[6]),
                        "PrecioTotal":float(item[7]),
                        "Costo1Nivel":float(item[8]),
                        "Precio1Nivel":float(item[9]),
                        "Cantidad":int(item[10])
                    })
            conn.close()

            # 2.- GET THE BODY & Partidas
            
            for item in wbs:
                # GET BODYS WSB 1.X
                if int(item['PartidaWBS'].count(".")) == 1:
                    cuerpos.append(item)
            
            # 2.1  EXPORT ALL CUERPOS
            #df_partida = pd.DataFrame(cuerpos)
            #df_partida.to_excel(f'C:\\NeodataReportesExcel\\cuerpos.xlsx', sheet_name='partidas', engine="openpyxl")
        
            #3 .- Partidas
            for rows_cuerpos in cuerpos:
                print(f"+ Cuerpo {rows_cuerpos['PartidaWBS']}:{rows_cuerpos['DescripcionPartidaLarga']}")
                cuerpo = rows_cuerpos['DescripcionPartidaLarga']
                for row_partidas in wbs:
                    if str(row_partidas['IdPartidaPadre']) == str(rows_cuerpos['IdPresupuestoPartida']) and row_partidas['Cantidad'] > 0:
                        print(f"    + Partida {row_partidas['PartidaWBS']}:{row_partidas['DescripcionPartidaLarga']}")
                        partida = row_partidas['DescripcionPartidaLarga']
                        nivel = ''
                        idBudgetHeading = row_partidas['IdPresupuestoPartida']
                        #3.1 - CHECK IF THERE ARE CONCEPTS IN PARTIDAS
                        if idBudgetHeading > 0:
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
                                            'Id':'0',
                                            'Crt':item[1],
                                            'Partida':partida,
                                            'Codigo':item[3],
                                            'C.Cliente':item[4],
                                            'Concepto':concept,
                                            'Unidad':item[6],
                                            'Cantidad':float(item[7]),
                                            'Volumen':item[8],
                                            'Kg_IM':item[9],
                                            'Kg':item[10],
                                            'Piezas':item[11],
                                            'Base Nivel':nivel,
                                            'Cuerpo':cuerpo,
                                            'Tipo':item[14],
                                            'Autor':item[15],
                                            'Observaciones':item[16],
                                            'Precio':float(item[17])
                                        })
                                conn.close()
                        #3.2 GET CONCEPT OF LEVEL
                        for row_nivel in wbs:
                            if str(row_nivel['IdPartidaPadre']) == str(row_partidas['IdPresupuestoPartida']) and row_nivel['Cantidad'] > 0:
                                print(f"        - Nivel {row_nivel['PartidaWBS']}:{row_nivel['DescripcionPartidaLarga']}")
                                nivel = row_nivel['DescripcionPartidaLarga']
                                idBudgetHeading = row_nivel['IdPresupuestoPartida']
                                if idBudgetHeading > 0:
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
                                                'Id':'0',
                                                'Crt':item[0],
                                                'Partida':partida,
                                                'Codigo':item[3],
                                                'C.Cliente':item[4],
                                                'Concepto':concept,
                                                'Unidad':item[6],
                                                'Cantidad':float(item[7]),
                                                'Volumen':item[8],
                                                'Kg_IM':item[9],
                                                'Kg':item[10],
                                                'Piezas':item[11],
                                                'Base Nivel':nivel,
                                                'Cuerpo':cuerpo,
                                                'Tipo':item[14],
                                                'Autor':item[15],
                                                'Observaciones':item[16],
                                                'Precio':float(item[17])
                                            })
                                    conn.close()
                                #3.3 CHECK SOME SUB-NIVEL
                                for row_sub_nivel in wbs:
                                    if  str(row_sub_nivel['IdPartidaPadre']) == str(row_nivel['IdPresupuestoPartida']) and row_sub_nivel['Cantidad'] > 0:
                                        partida = row_nivel['DescripcionPartidaLarga']
                                        nivel = row_sub_nivel['DescripcionPartidaLarga']
                                        idBudgetHeading = row_sub_nivel['IdPresupuestoPartida']
                                        if idBudgetHeading > 0:
                                            pass
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
                                                        'Id':'0',
                                                        'Crt':item[0],
                                                        'Partida':partida,
                                                        'Codigo':item[3],
                                                        'C.Cliente':item[4],
                                                        'Concepto':concept,
                                                        'Unidad':item[6],
                                                        'Cantidad':float(item[7]),
                                                        'Volumen':item[8],
                                                        'Kg_IM':item[9],
                                                        'Kg':item[10],
                                                        'Piezas':item[11],
                                                        'Base Nivel':nivel,
                                                        'Cuerpo':cuerpo,
                                                        'Tipo':item[14],
                                                        'Autor':item[15],
                                                        'Observaciones':item[16],
                                                        'Precio':float(item[17])
                                                    })
                                            conn.close()
                cuerpo = ''
                partida = ''
                nivel = ''
                
            #4 .- GET CONCEPTS
            #df_wsb = pd.DataFrame(data)
            #df_wsb.to_excel(f'C:\\NeodataReportesExcel\\presupuestos_ver.xlsx', sheet_name='presupuesto', engine="openpyxl")
        
        except Exception as err:
            logging.error(str(err))
        return data
    def getBudgeBodysEntriesLevesItems(self,db,idBudget):
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
        wbs = []
        bodies = []
        body_name = ''
        try:
            local_host = self.srv
            print(local_host)
            #1 .- GET THE MAIN NODE WSBP
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
                    "PartidasCosto.[Precio1Nivel],"\
                    "Partidas.[Cantidad] "\
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
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()

            if len(list(db_query)) > 0:
                for item in db_query:
                    wbs.append({
                        "IdPresupuestoPartida":item[0],
                        "PartidaWBS":item[1],
                        "IdPartidaPadre":item[2],
                        "DescripcionPartidaLarga":item[3],
                        "Costo":float(item[4]),
                        "Precio":float(item[5]),
                        "CostoTotal":float(item[6]),
                        "PrecioTotal":float(item[7]),
                        "Costo1Nivel":float(item[8]),
                        "Precio1Nivel":float(item[9]),
                        "Cantidad":float(item[10])})
            conn.close()

            # 1.- GET THE BODYs 1 = wsb only are int
            for item in wbs:
                if str(item['PartidaWBS']).count('.') == 0:
                    bodies.append({'id_partida_body':item['IdPresupuestoPartida'],
                        'id_body_wsp':str(item['PartidaWBS']),
                        'body':str(item['DescripcionPartidaLarga'])})
            
            # 2 .- GET THE ENTRIES (PARTIDAS)
            for body in bodies:
                print(f"+ ({body['id_body_wsp']}) {body['body']}")
                body_name = body['body']

                # 2.1 ENTRIES
                for entry in wbs:
                    if entry['IdPartidaPadre'] == body['id_partida_body'] and int(entry['Cantidad']) > 0:
                        print(f"    + ({entry['PartidaWBS']}) {entry['DescripcionPartidaLarga']}")
                        #HERE CODE FOR SEEK ITEMS
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
                                " Order By Crt desc".format(idBudget,entry['IdPresupuestoPartida'])
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
                                    'Id':'0',
                                    'Crt':item[0],
                                    'Partida':entry['DescripcionPartidaLarga'],
                                    'Codigo':item[3],
                                    'C.Cliente':item[4],
                                    'Concepto':concept,
                                    'Unidad':item[6],
                                    'Cantidad':float(item[7]),
                                    'Volumen':item[8],
                                    'Kg_IM':item[9],
                                    'Kg':item[10],
                                    'Piezas':item[11],
                                    'Base Nivel':'',
                                    'Cuerpo':body_name,
                                    'Tipo':item[14],
                                    'Autor':item[15],
                                    'Observaciones':item[16],
                                    'Precio':float(item[17])
                                })
                        conn.close()

                        # 2.3 GET LEVELS
                        for leves in wbs:
                            if str(leves['IdPartidaPadre']) == str(entry['IdPresupuestoPartida']) and int(leves['Cantidad']) > 0:
                                print(f"        - ({leves['PartidaWBS']}) {leves['DescripcionPartidaLarga']}")
                                #HERE CODE FOR SEEK ITEMS
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
                                " Order By Crt desc".format(idBudget,leves['IdPresupuestoPartida'])
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
                                            'Id':'0',
                                            'Crt':item[0],
                                            'Partida':entry['DescripcionPartidaLarga'],
                                            'Codigo':item[3],
                                            'C.Cliente':item[4],
                                            'Concepto':concept,
                                            'Unidad':item[6],
                                            'Cantidad':float(item[7]),
                                            'Volumen':item[8],
                                            'Kg_IM':item[9],
                                            'Kg':item[10],
                                            'Piezas':item[11],
                                            'Base Nivel':leves['DescripcionPartidaLarga'],
                                            'Cuerpo':body_name,
                                            'Tipo':item[14],
                                            'Autor':item[15],
                                            'Observaciones':item[16],
                                            'Precio':float(item[17])
                                        })
                                conn.close()
                                #EXTRA CHECK SEEK IF THERE ARE OTHER INDEX
                                for leves_extra in wbs:
                                    if str(leves_extra['IdPartidaPadre']) == str(leves['IdPresupuestoPartida']) and int(leves_extra['Cantidad']) > 0:
                                        print(f"            -- ({leves_extra['PartidaWBS']}) {leves_extra['DescripcionPartidaLarga']}")
                                        level_extra = leves['DescripcionPartidaLarga']
                                        entry_extra = leves_extra['DescripcionPartidaLarga']
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
                                            " Order By Crt desc".format(idBudget,leves_extra['IdPresupuestoPartida'])
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
                                                    'Id':'0',
                                                    'Crt':item[0],
                                                    'Partida':entry_extra,
                                                    'Codigo':item[3],
                                                    'C.Cliente':item[4],
                                                    'Concepto':concept,
                                                    'Unidad':item[6],
                                                    'Cantidad':float(item[7]),
                                                    'Volumen':item[8],
                                                    'Kg_IM':item[9],
                                                    'Kg':item[10],
                                                    'Piezas':item[11],
                                                    'Base Nivel':level_extra,
                                                    'Cuerpo':body_name,
                                                    'Tipo':item[14],
                                                    'Autor':item[15],
                                                    'Observaciones':item[16],
                                                    'Precio':float(item[17])
                                                })
                                        conn.close()

            #Test XLS
            #df_wsb = pd.DataFrame(data)
            #df_wsb.to_excel(f'C:\\NeodataReportesExcel\\presupuestos_2_ver.xlsx', sheet_name='presupuesto', engine="openpyxl")
        
        except Exception as err:
            logging.error(str(err))
        return data

    def getBudgeBodysEntriesLevesItemsExtra(self,db,idBudget):
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
        wbs = []
        bodies = []
        body_name = ''
        try:
            #1 .- GET THE MAIN NODE WSBP
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
                    "PartidasCosto.[Precio1Nivel],"\
                    "Partidas.[Cantidad] "\
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
            
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql)
            db_query = cursor.fetchall()

            if len(list(db_query)) > 0:
                for item in db_query:
                    wbs.append({
                        "IdPresupuestoPartida":item[0],
                        "PartidaWBS":item[1],
                        "IdPartidaPadre":item[2],
                        "DescripcionPartidaLarga":item[3],
                        "Costo":float(item[4]),
                        "Precio":float(item[5]),
                        "CostoTotal":float(item[6]),
                        "PrecioTotal":float(item[7]),
                        "Costo1Nivel":float(item[8]),
                        "Precio1Nivel":float(item[9]),
                        "Cantidad":float(item[10])})
            conn.close()

            # 1.- GET THE BODYs 1 = wsb only are int
            for item in wbs:
                if str(item['PartidaWBS']).count('.') == 0:
                    bodies.append({'id_partida_body':item['IdPresupuestoPartida'],
                        'id_body_wsp':str(item['PartidaWBS']),
                        'body':str(item['DescripcionPartidaLarga'])})
            entries =[]
            # 2 .- GET THE ENTRIES (PARTIDAS)
            for body in bodies:
                print(f"+ ({body['id_body_wsp']}) {body['body']}")
                body_name = body['body']

                # 2.1 ENTRIES
                for entry in wbs:
                    if entry['IdPartidaPadre'] == body['id_partida_body'] and int(entry['Cantidad']) > 0:
                        entries.append({
                                        'Id_partida':entry['IdPresupuestoPartida'],
                                        'PartidaWBS':entry['PartidaWBS'],
                                        'Partida':entry['DescripcionPartidaLarga']
                                    })
                #2.2 RUN ENTRIES AND GET CONCEPTS
                for items_entry in entries:
                    print(f"    + ({items_entry['PartidaWBS']}) {items_entry['Partida']}")
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
                        " Order By Crt desc".format(idBudget,items_entry['Id_partida'])

                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    db_query = cursor.fetchall()
                    if len(list(db_query)) > 0:
                        for item in db_query:
                            concept = str(item[5]).replace('\t','')
                            concept = concept.replace('\n','')
                            data.append({
                                'Id':'0',
                                'Crt':item[0],
                                'Partida':items_entry['Partida'],
                                'Codigo':item[3],
                                'C.Cliente':item[4],
                                'Concepto':concept,
                                'Unidad':item[6],
                                'Cantidad':float(item[7]),
                                'Volumen':item[8],
                                'Kg_IM':item[9],
                                'Kg':item[10],
                                'Piezas':item[11],
                                'Base Nivel':'',
                                'Cuerpo':body_name,
                                'Tipo':item[14],
                                'Autor':item[15],
                                'Observaciones':item[16],
                                'Precio':float(item[17])
                            })
                    conn.close()
                # 2.3 GET ENTRIERS - LEVES
                sub_leves = []
                for items_entry in entries:
                    print(f"    + ({items_entry['PartidaWBS']}) {items_entry['Partida']}")
                    for level in wbs:
                        if str(level['IdPartidaPadre']) == str(items_entry['Id_partida']) and int(level['Cantidad']) > 0:
                            print(f"        - ({level['PartidaWBS']}) {level['DescripcionPartidaLarga']}")
                            sub_leves.append({
                                'id_partida':int(items_entry['Id_partida']),
                                'partida':str(items_entry['Partida']),
                                'partida_WBS':str(items_entry['PartidaWBS']),
                                'id_level':int(level['IdPresupuestoPartida']),
                                'level':str(level['DescripcionPartidaLarga']),
                                'level_WBS':str(level['PartidaWBS'])
                            })
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
                                " Order By Crt desc".format(idBudget,level['IdPresupuestoPartida'])

                            conn = pyodbc.connect(conn_str)
                            cursor = conn.cursor()
                            cursor.execute(sql)
                            db_query = cursor.fetchall()
                            if len(list(db_query)) > 0:
                                for item in db_query:
                                    concept = str(item[5]).replace('\t','')
                                    concept = concept.replace('\n','')
                                    data.append({
                                        'Id':'0',
                                        'Crt':item[0],
                                        'Partida':items_entry['Partida'],
                                        'Codigo':item[3],
                                        'C.Cliente':item[4],
                                        'Concepto':concept,
                                        'Unidad':item[6],
                                        'Cantidad':float(item[7]),
                                        'Volumen':item[8],
                                        'Kg_IM':item[9],
                                        'Kg':item[10],
                                        'Piezas':item[11],
                                        'Base Nivel':level['DescripcionPartidaLarga'],
                                        'Cuerpo':body_name,
                                        'Tipo':item[14],
                                        'Autor':item[15],
                                        'Observaciones':item[16],
                                        'Precio':float(item[17])
                                    })
                            conn.close()

                # 2.3 SUB-LEVELs
                extra_level = []
                for sub_item in sub_leves:
                    print(f"+ ({sub_item['partida_WBS']}) {sub_item['partida']}")
                    print(f"    + ({sub_item['level_WBS']}) {sub_item['level']}")
                    for row in wbs:
                        if str(row['IdPartidaPadre']) == str(sub_item['id_level']) and int(row['Cantidad']) > 0:
                            print(f"        - ({row['PartidaWBS']}) {row['DescripcionPartidaLarga']}")
                            extra_level.append({
                                'id_partida':int(sub_item['id_partida']),
                                'partida':str(sub_item['partida']),
                                'partida_WBS':str(sub_item['partida_WBS']),
                                'id_level':int(sub_item['id_level']),
                                'level':str(sub_item['level']),
                                'level_WBS':str(sub_item['level_WBS']),
                                'id_level_extra':str(row['IdPresupuestoPartida']),
                                'level_extra':str(row['DescripcionPartidaLarga']),
                                'level_extra_WBS':str(row['PartidaWBS'])})
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
                                " Order By Crt desc".format(idBudget,row['IdPresupuestoPartida'])

                            conn = pyodbc.connect(conn_str)
                            cursor = conn.cursor()
                            cursor.execute(sql)
                            db_query = cursor.fetchall()
                            if len(list(db_query)) > 0:
                                for item in db_query:
                                    concept = str(item[5]).replace('\t','')
                                    concept = concept.replace('\n','')
                                    data.append({
                                        'Id':'0',
                                        'Crt':item[0],
                                        'Partida':sub_item['partida'],
                                        'Codigo':item[3],
                                        'C.Cliente':item[4],
                                        'Concepto':concept,
                                        'Unidad':item[6],
                                        'Cantidad':float(item[7]),
                                        'Volumen':item[8],
                                        'Kg_IM':item[9],
                                        'Kg':item[10],
                                        'Piezas':item[11],
                                        'Base Nivel':'',
                                        'Cuerpo':body_name,
                                        'Tipo':item[14],
                                        'Autor':item[15],
                                        'Observaciones':item[16],
                                        'Precio':float(item[17])
                                    })
                            conn.close()
                #2.4 FINAL LEVEL 
                for sub_item in extra_level:
                    print(f"+ ({sub_item['partida_WBS']}) {sub_item['partida']}")
                    print(f"    + ({sub_item['level_WBS']}) {sub_item['level']}")
                    for row in wbs:
                        if str(row['IdPartidaPadre']) == str(sub_item['id_level_extra']) and int(row['Cantidad']) > 0:
                            print(f"        - ({row['PartidaWBS']}) {row['DescripcionPartidaLarga']}")
                            
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
                                " Order By Crt desc".format(idBudget,row['IdPresupuestoPartida'])

                            conn = pyodbc.connect(conn_str)
                            cursor = conn.cursor()
                            cursor.execute(sql)
                            db_query = cursor.fetchall()
                            if len(list(db_query)) > 0:
                                for item in db_query:
                                    concept = str(item[5]).replace('\t','')
                                    concept = concept.replace('\n','')
                                    data.append({
                                        'Id':'0',
                                        'Crt':item[0],
                                        'Partida':sub_item['partida'],
                                        'Codigo':item[3],
                                        'C.Cliente':item[4],
                                        'Concepto':concept,
                                        'Unidad':item[6],
                                        'Cantidad':float(item[7]),
                                        'Volumen':item[8],
                                        'Kg_IM':item[9],
                                        'Kg':item[10],
                                        'Piezas':item[11],
                                        'Base Nivel':'',
                                        'Cuerpo':body_name,
                                        'Tipo':item[14],
                                        'Autor':item[15],
                                        'Observaciones':item[16],
                                        'Precio':float(item[17])
                                    })
                            conn.close()
            #Test XLS
            #df_wsb = pd.DataFrame(data)
            #df_wsb.to_excel(f'C:\\NeodataReportesExcel\\presupuestos_2_ver.xlsx', sheet_name='presupuesto', engine="openpyxl")
        
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