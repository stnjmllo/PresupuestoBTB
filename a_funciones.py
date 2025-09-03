import openpyxl
import pyodbc
import pandas as pd
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
def extraer_datos():

    # Consulta SQL utilizando el primer día del año presente
    query = f"""
    WITH VentasConRegion AS (
        SELECT 
            DimTiempo.Año,
            DimTiempo.Mes,
            DimClientes.NombreCliente,
            DimAlmacenes.Descripcion_Almacen,
            DimAlmacenes.Descripcion_Region,
            DimVendedorDestino.NombreVendedorDestino,
            DimClientes.Id_Cliente,
            DimClientes.TipoCliente,
            DimProductos.Marca,
            FactVentas.EnumerarDescuento,
            FactVentas.Cantidad,
            FactVentas.Subtotal,
            FactVentas.CanalDestino,

            -- Asignar Región BTOB con CASE (corregido y completo)
            CASE 
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B025','B004','B005','B003','B007','B016','B006','B002',
                    'B031','B034','B036','B038','B039','B043','B044','B047',
                    'P622','B057','P637'
                ) THEN 'CENTRO'
                
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B001','B019','B020','B017','B018','B041'
                ) THEN 'NORTE'
                
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B026','B023','B029','B030','B014','B015','P073','B035',
                    'B040','B045','B048','B052','B053','B054','B058','P654'
                ) THEN 'OCCIDENTE'
                
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B012','B011','B013','B046','B051'
                ) THEN 'ORIENTE'
                
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B028','B027','B024','B009','B008','B010','B021','B033',
                    'B037','B042','BT03','P038'
                ) THEN 'SUR'
                
                WHEN DimVendedorDestino.CodigoVendedorDestino IN (
                    'B032','B055'
                ) THEN 'CADENAS'
                
                ELSE 'Indeterminado'
            END AS RegionBTOB
        FROM FactVentas
        LEFT JOIN DimTiempo ON FactVentas.Snk_Tiempo = DimTiempo.SnkTiempo
        LEFT JOIN DimAlmacenes ON FactVentas.Snk_Almacen = DimAlmacenes.SnkAlmacenes
        LEFT JOIN DimVendedorDestino ON FactVentas.Snk_Vendedor_Destino = DimVendedorDestino.SnkVendedorDestino
        LEFT JOIN DimClientes ON FactVentas.Snk_Cliente = DimClientes.SnkClientes
        LEFT JOIN DimProductos ON FactVentas.Snk_Producto = DimProductos.SnkProductos
    )
    SELECT 
        Año,
        Mes,
        NombreCliente,
        Descripcion_Almacen,
        Descripcion_Region,
        NombreVendedorDestino,
        Id_Cliente,
        TipoCliente,
        Marca,
        EnumerarDescuento,
        RegionBTOB,
        SUM(Cantidad) AS TotalCantidad,
        SUM(Subtotal) AS TotalSubtotal
    FROM VentasConRegion
    WHERE 
        Año >= 2024
        AND CanalDestino = 'BTOB'
        AND NombreVendedorDestino NOT IN ('VIVELL SAS', 'Indeterminado')
        AND RegionBTOB <> 'Indeterminado'
        AND EnumerarDescuento LIKE '%1%'
    GROUP BY 
        Año,
        Mes,
        NombreCliente,
        Descripcion_Almacen,
        Descripcion_Region,
        NombreVendedorDestino,
        Id_Cliente,
        TipoCliente,
        Marca,
        EnumerarDescuento,
        RegionBTOB;


    """

    try:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=amaterasu\siesa;DATABASE=reportes;Trusted_Connection=yes;')
        print('conexion exitosa')
    except Exception as ex:
        print(ex)

        cursor = conn.cursor()

    return pd.read_sql(query, conn)

def limpiar_espacios(df, columna):
    df[columna] = df[columna].str.strip()                # elimina espacios al inicio y final
    df[columna] = df[columna].str.replace(r'\s+', ' ', regex=True)  # reemplaza múltiples espacios por uno solo
    return df

def limp_trans(df):
    Almacenes_clean=df
    
    Almacenes_clean = Almacenes_clean[(Almacenes_clean['Marca'] != 'NO APLICA') & (Almacenes_clean['Marca'] != 'Indeterminado')]

    Almacenes_clean = Almacenes_clean[Almacenes_clean['TipoCliente'] != 'EMPLEADO']

    Almacenes_clean.loc[Almacenes_clean['TipoCliente'] == 'POS', 'NombreCliente'] = '-'
    filas_antes = len(Almacenes_clean)

    # Estandarizar nombres de vendedores
    Almacenes_clean.replace({
        'JEISON ARRIETA': 'ARRIETA GALVIS JEISSON ARLEY',
        'CINDY MEJIA': 'MEJIA LOBO CINDY DORIS',
        'CLARIVETH MACHACON': 'MACHACON PEREZ CLARIVETH',
        'ROSIRIS JULIO': 'JULIO PADILLA ROSIRIS ELENA',
        'INES ARANGO': 'ARANGO DE HOYOS INES PATRICIA',
        'ARIAS DUQUE NUBIA ESMERALDA': 'ARIAS DUQUE NUBIA ESMERALDA',
        'DANIELA ARCILA': 'ARCILA CHICA DANIELA',
        'ESTEFANY CHICA': 'CHICA MONTOYA ESTEFANY',
        'ARCILA CHICA DANIELA\t': 'ARCILA CHICA DANIELA',
    }, inplace=True)

    # Agrupar tiendas Falabella
    Almacenes_clean.replace({
        'FALABELLA DE COLOMBIA S A- GALERIA': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A- LA FELICIDAD': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A - CARACOLI:': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A - COLINA': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A - SAN DIEGO': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A- EL CASTILLO': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A -FONTANAR': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A- SANTAFE': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A - CARACOLI': 'FALABELLA DE COLOMBIA S A',
        'FALABELLA DE COLOMBIA S A- BUENAVISTA': 'FALABELLA DE COLOMBIA S A'
    }, inplace=True)

    # Agrupar tiendas Marrocar
    Almacenes_clean.replace({
        'MARROCAR CATALOGO S.A.S': 'MARROCAR',
        'MARROCAR BOLIVAR S.A.S': 'MARROCAR',
        'MARROCAR VERACRUZ S.A.S': 'MARROCAR',
        'MARROCAR MALCA2 S.A.S': 'MARROCAR',
        'MARROCAR PEREIRA SAS': 'MARROCAR',
        'MARROCAR NEIVA': 'MARROCAR',
        'MARROCAR MANIZALES': 'MARROCAR',
        'MARROCAR CAMINO REAL S.A.S': 'MARROCAR',
        'MARROCAR ONLINE S.A.S': 'MARROCAR',
        'MARROCAR SOACHA': 'MARROCAR'
    }, inplace=True)

    Almacenes_clean.rename(columns={'TotalSubtotal':'PESOS','TotalCantidad':'UND'},inplace=True)

    POS=Almacenes_clean[Almacenes_clean['TipoCliente']=='POS']
    POS['Vendedor']=POS['NombreVendedorDestino']
    POS['Id_Cliente']=''
    POS['Rank']=1

    POS=POS.replace({'ARANGO AYALA DEICY YULIANA':'ROJAS TOQUICA YULI ESTEFANI',
    'DE LOS RIOS SALDARRIAGA DIANA MARCELA':'ARCILA CHICA DANIELA',
    'GALINDO ARAQUE CARLOS ALFREDO':'ROJAS TOQUICA YULI ESTEFANI',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA',
    'SANCLEMENTE SANCHEZ ELVIRA':'PINTO HIJINIO MARYI ALEJANDRA',})

    Almacenes_clean=Almacenes_clean.replace({'ARANGO AYALA DEICY YULIANA':'ROJAS TOQUICA YULI ESTEFANI',
    'DE LOS RIOS SALDARRIAGA DIANA MARCELA':'ARCILA CHICA DANIELA',
    'GALINDO ARAQUE CARLOS ALFREDO':'ROJAS TOQUICA YULI ESTEFANI',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA',
    'SANCLEMENTE SANCHEZ ELVIRA':'PINTO HIJINIO MARYI ALEJANDRA',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA'})

    POS.replace({'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA'},inplace=True)
    Almacenes_clean.replace({'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA'},inplace=True)

    POS['Rank']=1

    POS['Id_Cliente']='-'

    filtro = Almacenes_clean['NombreCliente'].str.contains('falabella', case=False, na=False)

    Almacenes_clean.loc[filtro, 'TipoCliente'] = 'GRAN SUPERFICIE'
    Almacenes_clean.loc[filtro, 'NombreCliente'] = 'FALABELLA DE COLOMBIA S A'

    filtro = Almacenes_clean['NombreCliente'].str.contains('TEXVIDA', case=False, na=False)

    Almacenes_clean.loc[filtro, 'NombreCliente'] = 'TEXVIDA S.A.S.'

    Almacenes_clean=Almacenes_clean[Almacenes_clean['TipoCliente']!='POS']

    Almacenes_clean=Almacenes_clean.rename(columns={'TotalCantidad':'UND','TotalSubtotal':'PESOS'})

    POS=POS.replace({'ARANGO AYALA DEICY YULIANA':'ROJAS TOQUICA YULI ESTEFANI',
    'DE LOS RIOS SALDARRIAGA DIANA MARCELA':'ARCILA CHICA DANIELA',
    'GALINDO ARAQUE CARLOS ALFREDO':'ROJAS TOQUICA YULI ESTEFANI',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA',
    'SANCLEMENTE SANCHEZ ELVIRA':'PINTO HIJINIO MARYI ALEJANDRA',})

    Almacenes_clean=Almacenes_clean.replace({'ARANGO AYALA DEICY YULIANA':'ROJAS TOQUICA YULI ESTEFANI',
    'DE LOS RIOS SALDARRIAGA DIANA MARCELA':'ARCILA CHICA DANIELA',
    'GALINDO ARAQUE CARLOS ALFREDO':'ROJAS TOQUICA YULI ESTEFANI',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA',
    'SANCLEMENTE SANCHEZ ELVIRA':'PINTO HIJINIO MARYI ALEJANDRA',
    'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA'})

    POS.replace({'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA','ARCILA CHICA DANIELA\t':'ARCILA CHICA DANIELA'},inplace=True)
    Almacenes_clean.replace({'ELVIRA SANCLEMENTE':'PINTO HIJINIO MARYI ALEJANDRA'},inplace=True)

    POS['Rank']=1

    POS['Id_Cliente']='-'

    filtro = Almacenes_clean['NombreCliente'].str.contains('falabella', case=False, na=False)

    Almacenes_clean.loc[filtro, 'TipoCliente'] = 'GRAN SUPERFICIE'
    Almacenes_clean.loc[filtro, 'NombreCliente'] = 'FALABELLA DE COLOMBIA S A'

    filtro = Almacenes_clean['NombreCliente'].str.contains('TEXVIDA', case=False, na=False)

    Almacenes_clean.loc[filtro, 'NombreCliente'] = 'TEXVIDA S.A.S.'

    Almacenes_clean=Almacenes_clean[Almacenes_clean['TipoCliente']!='POS']

    Almacenes_clean=Almacenes_clean.rename(columns={'TotalCantidad':'UND','TotalSubtotal':'PESOS'})
    return Almacenes_clean,POS

def rank(Almacenes_clean):
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import pandas as pd
    from dateutil.relativedelta import relativedelta

    # Paso 0: Copiar base de datos
    df = Almacenes_clean.copy()
    # Fecha actual
    hoy = datetime.today()

    # Fecha de corte: 4 meses atrás
    fecha_corte = hoy - relativedelta(months=3)

    # Año y mes de corte
    año_corte = fecha_corte.year
    mes_corte = fecha_corte.month

    # Filtrar desde año y mes de corte
    df_2025 = df[(df['Año'] > año_corte) |
                    ((df['Año'] == año_corte) & (df['Mes'] >= mes_corte))].copy()

    # Paso 2: Calcular ventas por cliente en 2025
    ranking = df_2025.groupby(['NombreVendedorDestino', 'RegionBTOB', 'TipoCliente', 'NombreCliente'], as_index=False)['PESOS'].sum()

    # Paso 3: Rankear clientes dentro de su grupo
    ranking['Rank'] = ranking.groupby(['NombreVendedorDestino', 'RegionBTOB', 'TipoCliente'])['PESOS'] \
                            .rank(method='first', ascending=False)

    # Paso 4: Contar cuántos clientes únicos hay por grupo y añadir columna TotalClientes directamente
    clientes_por_grupo = ranking.groupby(['NombreVendedorDestino', 'RegionBTOB', 'TipoCliente'])['NombreCliente'] \
                                .nunique().reset_index(name='TotalClientes')

    # Paso 5: Unir info de clientes únicos al ranking
    ranking = ranking.merge(clientes_por_grupo, on=['NombreVendedorDestino', 'RegionBTOB', 'TipoCliente'], how='left')

    # Paso 6: Crear flag "es_top5" solo si hay más de 5 clientes y Rank <= 5
    ranking['es_top5'] = np.where((ranking['TotalClientes'] > 5) & (ranking['Rank'] <= 5), True,
                                np.where(ranking['TotalClientes'] > 5, False, True))

    # Paso 7: Crear clave única por grupo + cliente
    ranking['clave'] = ranking['NombreVendedorDestino'] + "|" + ranking['RegionBTOB'] + "|" + ranking['TipoCliente'] + "|" + ranking['NombreCliente']
    top5_claves = set(ranking[ranking['es_top5']]['clave'])

    # Paso 8: Aplicar lógica al dataset completo
    df['clave'] = df['NombreVendedorDestino'] + "|" + df['RegionBTOB'] + "|" + df['TipoCliente'] + "|" + df['NombreCliente']
    df['NombreCliente'] = np.where(df['clave'].isin(top5_claves), df['NombreCliente'], '_OTROS')
    df.drop(columns=['clave'], inplace=True)
    return df

def tran2(df, POS):
    df_final=df
    df_final=df_final[['Año',	'Mes',	'RegionBTOB',	'NombreVendedorDestino',	'TipoCliente',	'NombreCliente',	'PESOS',	'UND']]
    POS=POS[['Año',	'Mes',	'RegionBTOB',	'NombreVendedorDestino',	'TipoCliente',	'NombreCliente',	'PESOS',	'UND']]
    POS=POS.rename(columns={'TotalSubtotal':'PESOS','TotalCantidad':'UND'})
    POS=POS.groupby(['Año', 'Mes', 'RegionBTOB', 'NombreVendedorDestino', 'TipoCliente', 'NombreCliente'])[['PESOS', 'UND']].sum().reset_index()
    POS=POS.rename(columns={'Vendedor':'FINAL'})
    POS['NombreCliente']='-'

    # Ensure column names are unique
    POS=POS.groupby(['Año', 'Mes', 'RegionBTOB', 'NombreVendedorDestino', 'TipoCliente', 'NombreCliente'])[['PESOS', 'UND']].sum().reset_index()


    # Concatenar verticalmente (una debajo de otra)
    df_total = pd.concat([df_final, POS], ignore_index=True)

    df_final=df_total

    filtro = (df_final['NombreVendedorDestino'] == 'ALEGRIA REYES KELLY JOHANA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'SUR'

    filtro = (df_final['NombreVendedorDestino'] == 'ARANGO DE HOYOS INES PATRICIA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARCILA CHICA DANIELA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'OCCIDENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ROJAS TOQUICA YULI ESTEFANI') 

    df_final.loc[filtro, 'RegionBTOB'] = 'CENTRO'

    filtro = (df_final['NombreVendedorDestino'] == 'POSADA DIAZ KAREN DAYANA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'CENTRO'

    filtro = (df_final['NombreVendedorDestino'] == 'CRISTANCHO  YENNY CAROLINA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'CENTRO'

    filtro = (df_final['NombreVendedorDestino'] == 'CARDENAS CASTAÑEDA LEIDY JOHANNA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'CENTRO'

    filtro = (df_final['NombreVendedorDestino'] == 'MEJIA LOBO CINDY DORIS') 

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'MACHACON PEREZ CLARIVETH') 

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'MACHACON PEREZ CLARIVETH') 

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'JULIO PADILLA ROSIRIS ELENA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARANGO DE HOYOS INES PATRICIA')

    df_final.loc[filtro, 'RegionBTOB'] = 'NORTE'

    filtro = (df_final['NombreVendedorDestino'] == 'CHICA MONTOYA ESTEFANY') 

    df_final.loc[filtro, 'RegionBTOB'] = 'OCCIDENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARRIETA GALVIS JEISSON ARLEY') 

    df_final.loc[filtro, 'RegionBTOB'] = 'OCCIDENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARRIETA GALVIS JEISSON ARLEY') 
    df_final.loc[filtro, 'RegionBTOB'] = 'OCCIDENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'FONRODONA MANTILLA MARIA FERNANDA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'ORIENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARIAS DUQUE NUBIA ESMERALDA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'ORIENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'ARCHILA RAMIREZ LUZ STELLA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'ORIENTE'

    filtro = (df_final['NombreVendedorDestino'] == 'PINTO HIJINIO MARYI ALEJANDRA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'SUR'

    filtro = (df_final['NombreVendedorDestino'] == 'ALEGRIA REYES KELLY JOHANA') 

    df_final.loc[filtro, 'RegionBTOB'] = 'SUR'

    df_total=df_final
    df_total=df_total.dropna()
    df_total.replace({'JEISON ARRIETA':'ARRIETA GALVIS JEISSON ARLEY','CINDY MEJIA':'MEJIA LOBO CINDY DORIS','CLARIVETH MACHACON':'MACHACON PEREZ CLARIVETH',
                        'ROSIRIS JULIO':'JULIO PADILLA ROSIRIS ELENA','INES ARANGO':'ARANGO DE HOYOS INES PATRICIA',
                        'ARIAS DUQUE NUBIA ESMERALDA':'ARIAS DUQUE NUBIA ESMERALDA','DANIELA ARCILA':'ARCILA CHICA DANIELA',
                        'ESTEFANY CHICA':'CHICA MONTOYA ESTEFANY'},
                        inplace=True)
    return df_total, df_final

def resumen_pivot(df_total):

    import pandas as pd

    pivot_df = pd.pivot_table(
        df_total,
        values=['PESOS', 'UND'],
        index=['RegionBTOB', 'NombreVendedorDestino', 'TipoCliente', 'NombreCliente'],
        columns=['Año', 'Mes'],
        aggfunc="mean",
        fill_value=0
    )

    # Reordena para que quede Año -> Mes -> Variable
    pivot_df = pivot_df.reorder_levels([1, 2, 0], axis=1).sort_index(axis=1)

    # Quita el MultiIndex de columnas si quieres (opcional)
    pivot_df = pivot_df.reset_index()

    from datetime import datetime

    # Última columna (tuple: Año, Mes, Métrica)
    ultimo_anio, ultimo_mes, _ = pivot_df.columns[-1]

    # Fecha actual
    hoy = datetime.today()

    # Validamos si corresponde al mes actual y antes del 25
    if hoy.year == int(ultimo_anio) and hoy.month == int(ultimo_mes) and hoy.day <= 25:
        # Seleccionamos todas las columnas de ese año y mes (PESOS y UND)
        cols_a_borrar = [col for col in pivot_df.columns if col[0] == ultimo_anio and col[1] == ultimo_mes]
        
        # Borramos esas columnas
        pivot_df = pivot_df.drop(columns=cols_a_borrar)
        
    pivot_df["SumaCols"] = pivot_df.iloc[:, [-2, -4, -6]].sum(axis=1)

    pivot_df["VALOR POR UND ULTIMO PERIODO"] = ((pivot_df.iloc[:, [-4, -6]].sum(axis=1))/(pivot_df.iloc[:, [-3, -5]].sum(axis=1))).round(2)

    import numpy as np
    import pandas as pd

    # Asegura que SumaCols sea numérica
    pivot_df["SumaCols"] = pd.to_numeric(pivot_df["SumaCols"], errors="coerce").fillna(0)

    claves = ["RegionBTOB", "NombreVendedorDestino", "TipoCliente"]

    # Total por grupo (sin NombreCliente)
    pivot_df["TotalGrupo"] = pivot_df.groupby(claves)["SumaCols"].transform("sum")

    # Peso de cada línea: SumaCols / Total del grupo
    pivot_df["PESO DE VENDEDOR POR CLIENTE"] = np.where(
        pivot_df["TotalGrupo"] != 0,
        pivot_df["SumaCols"] / pivot_df["TotalGrupo"],
        0.0
    )

    pivot_df.fillna(0, inplace=True)
    return pivot_df
