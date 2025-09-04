#  Presupuesto BTB
Aplicación web en **Flask** que permite extraer, transformar y generar reportes en **Excel** sobre las ventas BTOB. El proyecto automatiza la conexión a la base de datos Siesa, aplica reglas de negocio para limpieza y ranking de clientes, y genera un archivo consolidado listo para análisis.

##  Características
- Conexión a **SQL Server** mediante `pyodbc`.
- Limpieza y normalización de datos de clientes y vendedores.
- Generación de ranking de clientes y agrupación por región BTOB.
- Creación de tablas dinámicas (pivot tables) con métricas:  **PESOS** (ventas en dinero),  **UND** (ventas en unidades), Valor promedio por unidad,  Peso de cada cliente frente a su grupo.
- Interfaz web en Flask con Bootstrap: botón único para ejecutar el pipeline completo y descarga automática del archivo Excel generado.

##  Requisitos
- **Python 3.10+**
- Librerías: `flask`, `pandas`, `numpy`, `pyodbc`, `openpyxl`, `python-dateutil`

Instálalas con:
```bash
pip install -r requirements.txt
```

##  Estructura del proyecto
```
PresupuestoBTB/
├── a_funciones.py      # Funciones de ETL (extracción, transformación, limpieza)
├── app.py              # Aplicación Flask con la interfaz web
├── requirements.txt    # Dependencias del proyecto
├── static/             # Recursos estáticos (logo, estilos)
├── RESULTADOS/         # Carpeta donde se generan los Excel
└── README.md           # Documentación del proyecto
```

##  Uso
1. Clona el repositorio:
```bash
git clone https://github.com/stnjmllo/PresupuestoBTB.git
cd PresupuestoBTB
```
2. Activa el entorno virtual e instala dependencias:
```bash
python -m venv venv
source venv/Scripts/activate   # en Windows
source venv/bin/activate       # en Linux/Mac
pip install -r requirements.txt
```
3. Ejecuta la aplicación:
```bash
python app.py
```
4. Abre en tu navegador:  
`http://127.0.0.1:5000`
5. Presiona el botón **"Ejecutar y descargar"**. Se generará un archivo Excel en la carpeta `RESULTADOS/` y se descargará como `datos.xlsx`.

##  Notas técnicas
- La exportación a Excel se hace con `pandas.to_excel`, usando `openpyxl`.
- Los archivos se guardan con un timestamp en el servidor (`datos_YYYYMMDD_HHMMSS.xlsx`) pero se descargan siempre como `datos.xlsx` para mayor comodidad.
- Los warnings de `SettingWithCopyWarning` en Pandas son normales en esta versión, no afectan el resultado final.

##  Autores
**Melina Muñoz Marín**  
Área de Sistemas — Vivell S.A.S  
 help.desk@vivell.co

**Juan Sebastian Jaramillo Gomez**  
Área de Sistemas — Vivell S.A.S  
 aux.bi1@vivell.co

##  Licencia
Este proyecto es de uso interno para **Vivell S.A.S**.  
No distribuir sin autorización.
