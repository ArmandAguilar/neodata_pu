# Neodata Precios Unitarios SDK
[![Supported Versions](https://img.shields.io/pypi/pyversions/requests.svg)]()
[![License](https://img.shields.io/npm/l/express.svg)]()


## Dependency

```python
pip install requests
pip install pandas
pip install pyodbc
pip install pymssql
pip install xlsxwriter
pip install openpyxl
pip install wheel
```

## Build the lib.
into the folder run 

```cmd
python -m build
```
seek the folder '\dist\neodata_pu-1.0.1-py3-none-any.whl' and install
```cmd
pip install \dist\neodata_pu-1.0.1-py3-none-any.whl
```

## Auto Install

```Install For Windows
install.bat
```

## Import

```python
from neodata_pu import neodataPu
```

## Initialize Neodata Object
<b>path_script:</b> This path are in  C:\\NEODATA2021\\DB\\Scripts or C:\\NEODATAWinPlus\\DB\\Scripts,the objective is seek the file called SQLCMD.exe.

```python
    path = 'C:\\NEODATAWinPlus\\BD\\Scripts\\'
    neodataPu(path_script=path)
```
If you have the server conexion you could set in the var server.
```python
    path = 'C:\\NEODATAWinPlus\\BD\\Scripts\\'
    srv = 'np:\\.\pipe\LOCALDB#84702F74\tsql\query'
    neodataPu(path_script=path,server=srv)
```
<p>&nbsp;</p>


### GET A LIST OF ALL NEODATA'S DB

```python

neodata.getListDb()
	
```

```json

[
    { name:'C:\\FOO.mdf' },
    { name:'C:\\BUDGET_ONE.mdf' },
    { name:'C:\\BUDGET_TWO.mdf' },
]
	
```
<p>&nbsp;</p>

### GET ALL VERSIONS OF BUDGET

```python

    neodata.getVersions(db='C:\\BUDGET_ONE.mdf')
	
```
```json

[
    { IdPresupuesto:'1',Presupuesto:'BUDGET V1' },
    { IdPresupuesto:'2',Presupuesto:'BUDGET V2' },
    { IdPresupuesto:'3',Presupuesto:'BUDGET V3' },
]
	
```
<p>&nbsp;</p>

### GET ALL WSB PANEL
```python

    neodata.getPartidasWSB(db='C:\\BUDGET_ONE.mdf',
                            idBudget='1'
	
```
```json

[
    {
        'IdPresupuestoPartida':5,
        'PartidaWBS':'1.1',
        'IdPartidaPadre':2,
        'DescripcionPartidaLarga':'CONCRETO',
    }
]
	
```

### GET ALL WSB PANEL WITH COST
```python

    neodata.getPartidasWSBWithCost(db='C:\\BUDGET_ONE.mdf',
                            idBudget='1'
	
```
```json

[
    {
        'IdPresupuestoPartida':5,
        'PartidaWBS':'1.1',
        'IdPartidaPadre':2,
        'DescripcionPartidaLarga':'CONCRETO',
        'Costo':1000.23,
        'Precio':1000.23,,
        'CostoTotal':1000.23,
        'PrecioTotal':1000.23,
        'Costo1Nivel':1000.23,
        'Precio1Nivel':1000.23
    }
]
	
```
### GET ALL Budget Items
```python

    neodata.getBudgetItems(db='C:\\BUDGET_ONE.mdf',
                            idBudget='1'
	
```
```json

[
    {
        'IdPresupuestoConcepto':item,
        'IdPresupuesto':item,
        'IdPresupuestoPartida':item,
        'IdExpIns':item,
        'IdEstimacionTipo':item,
        'IdAgrupador':item,
        'Control':item,
        'Renglon':item,
        'Cantidad':item,
        'CantidadTotal':item,
        'Marca':item,
        'CodigoAuxiliar':item,
        'DescripcionAuxiliar':item,
        'IndirectoEspecial':item,
        'PorcentajeIndirectoEspecial':item,
        'RutaGenerador':item,
        'AEstimar':item,
        'YaEstimado':item,
        'Programado':item,
        'IdCentro':item,
        'IdContratista':item,
        'Bloqueado':item,
        }
]
	
```
## Special Report Level Indent
idBudget = This var is the id of budget

<b>Type Nodo 1</b>
```
(+) Budget WSB 1
    (+) Body A WSB 1.1
        (+) Entry 1 WSB 1.1.01
            (-) Level 1 WSB 1.1.01.001
    (+) Body B WSB 1.2
        (+) Entry 1 WSB 1.2.01
            (-) Level 1 WSB 1.1.02.001
```

```python
    neodata.getBudgeBodySimpleIndet(db='C:\\BUDGET_ONE.mdf',idBudget='1')
```

```json
[
    {
        'Id':item,
        'Crt':item,
        'Partida':item,
        'Codigo':item,
        'C.Cliente':item,
        'Concepto':concept,
        'Unidad':item,
        'Cantidad':item,
        'Volumen':item,
        'Kg_IM':item,
        'Kg':item,
        'Piezas':item,
        'Base Nivel':item,
        'Cuerpo':item,
        'Tipo':item,
        'Autor':item,
        'Observaciones':item,
        'Precio':item    
    }
]
	
```

<b>Type Nodo 2</b>
```
(+) Cuerpo WSB 1
    (+) Partida WSB 1.01
        (-) Nivel WSB 1.01.001
        (-) Nivel WSB 1.01.002
        (-) Nivel WSB 1.01.003
    (+) Partida WSB 1.02
        (-) Nivel WSB 1.02.001
```

```python
    neodata.getBudgeBodysEntriesLevesItems(db='C:\\BUDGET_ONE.mdf',idBudget='1')
```

```json
[
    {
        'Id':item,
        'Crt':item,
        'Partida':item,
        'Codigo':item,
        'C.Cliente':item,
        'Concepto':concept,
        'Unidad':item,
        'Cantidad':item,
        'Volumen':item,
        'Kg_IM':item,
        'Kg':item,
        'Piezas':item,
        'Base Nivel':item,
        'Cuerpo':item,
        'Tipo':item,
        'Autor':item,
        'Observaciones':item,
        'Precio':item    
    }
]
	
```


### GET Catalog
```python
    neodata.getCatalogo(db='C:\\BUDGET_ONE.mdf')
	
```
```json
[
    {
        'IdCodigo':item,
        'Codigo':item,
        'Descripcion':item,
        'IdUnidad':item,
        'Unidad':item,
        'Descripcion_Unidad':item,
    }
]
	
```


## 🍺 Buy me a beer

|BTC|LTC|
|--|--|
|<img src="http://armando-aguilar.com/wp-content/uploads/2022/07/wallet_btc.png" width="150">|<img src="http://armando-aguilar.com/wp-content/uploads/2022/07/wallet_ltc.png" width="150">|
|1JDA4DEkVnB9rvLgLD5Xe9jfU2pJtnCZiG|LhBrMcs7i3mD2wjiUv3KGtx9eEQeyBE4Dg|

<p>&nbsp;</p>


