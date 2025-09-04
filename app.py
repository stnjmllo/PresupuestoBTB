# ============================================================
# Realizado por: Melina Muñoz Marín
# Área de Sistemas — Vivell S.A.S
# Contacto: help.desk@vivell.co
# ============================================================

# app.py
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template_string, send_file
import a_funciones as fun  # ← Módulo con la lógica de extracción y transformación

app = Flask(__name__)

# =========================
# Plantilla HTML embebida
# =========================
# - Incluye Bootstrap para estilos
# - Muestra el logo
# - Muestra un banner superior con el crédito solicitado
# - Botón que invoca el endpoint POST /descargar y dispara la descarga del Excel
INDEX_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body{ background:#f7f7f9; min-height:100vh; display:flex; flex-direction:column; }
    .logo-vivell{
      display:block; margin:28px auto 8px; max-width:660px; width:85vw; height:auto;
    }
    .credit{
      text-align:center; font-size:.92rem; color:#495057; margin-top:6px;
    }
    .main-wrap{ flex:1; display:flex; align-items:center; justify-content:center; }
    .card{ border:none; border-radius:1.25rem; box-shadow:0 6px 24px rgba(0,0,0,.08); }
    .btn-run{ padding:.9rem 1.5rem; font-weight:600; font-size:1.05rem; display:flex; align-items:center; justify-content:center; gap:.6rem; width:100%; border-radius:.8rem; transition:all .2s; }
    .btn-run:hover{ transform:translateY(-2px); box-shadow:0 6px 16px rgba(0,0,0,.2); }
    .spinner-border{ width:1.2rem; height:1.2rem; border-width:.15rem; }
    footer{ color:#6c757d; font-size:.85rem; text-align:center; margin:10px 0 10px; }
  </style>
</head>
<body>
  <!-- Logo -->
  <img src="{{ url_for('static', filename='VIVELL MELINA (1).png') }}" alt="Vivell Logo" class="logo-vivell">
  <!-- Crédito solicitado (arriba) -->
  <div class="credit">
    Realizado por <strong>Melina Muñoz Marín</strong> — Área de Sistemas ·
    <a href="mailto:help.desk@vivell.co">help.desk@vivell.co</a>
  </div>

  <!-- Tarjeta principal con botón -->
  <div class="container main-wrap">
    <div class="row justify-content-center w-100">
      <div class="col-md-7 col-lg-5">
        <div class="card p-3 p-md-4 text-center">
          <h1 class="mb-2"></h1>
          <p class="text-muted mb-3">Presiona el botón para correr el proceso y descargar el Excel.</p>

          <button id="runBtn" class="btn btn-primary btn-lg btn-run" onclick="descargar()">
            <span id="spinner" class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
            <span id="runText">Ejecutar y descargar</span>
          </button>

          <div id="msg" class="mt-2 small text-muted"></div>
        </div>
      </div>
    </div>
  </div>

  <footer>Hecho por el área de sistemas Vivell S.A.S</footer>

  <!-- JS: maneja la llamada POST /descargar y fuerza la descarga del archivo -->
  <script>
    const runBtn  = document.getElementById('runBtn');
    const spinner = document.getElementById('spinner');
    const runText = document.getElementById('runText');
    const msg     = document.getElementById('msg');

    async function descargar() {
      // Estado "procesando"
      runBtn.disabled = true;
      spinner.classList.remove('d-none');
      runText.textContent = 'Procesando...';
      msg.textContent = '';

      try {
        // Llama al endpoint que genera el Excel
        const resp = await fetch('/descargar', { method: 'POST' });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || ('HTTP ' + resp.status));
        }

        // Intenta obtener el nombre de archivo desde el header
        const dispo = resp.headers.get('Content-Disposition') || '';
        let filename = 'archivo.xlsx';
        const match = /filename\\*=UTF-8''([^;]+)|filename=\\"?([^\\\";]+)\\"?/i.exec(dispo);
        if (match) filename = decodeURIComponent((match[1] || match[2] || 'archivo.xlsx'));

        // Fuerza descarga en el navegador
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = filename; document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);

        msg.textContent = '✅ Descarga iniciada: ' + filename;
      } catch (err) {
        console.error(err);
        msg.textContent = '❌ ' + (err.message || err);
      } finally {
        // Restablece estado del botón
        spinner.classList.add('d-none');
        runText.textContent = 'Ejecutar y descargar';
        runBtn.disabled = false;
      }
    }
  </script>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# =========================
# Lógica del pipeline
# =========================
def ejecutar_pipeline():
    """
    Orquesta el flujo completo:
      1) Extrae datos desde SQL (a_funciones.extraer_datos).
      2) Limpia espacios en el nombre del vendedor.
      3) Aplica transformaciones de negocio (limp_trans, rank, tran2).
      4) Crea el pivot/resumen con MultiIndex (Año/Mes/Métrica).
      5) Escribe un Excel con nombre en disco con timestamp, p.ej. RESULTADOS/datos_YYYYmmdd_HHMMSS.xlsx.
    Retorna: Path al archivo generado en disco.
    """

    # 1) Extraer datos crudos desde SQL Server
    df = fun.extraer_datos()

    # 2) Normalizar espacios en el campo de vendedor
    df = fun.limpiar_espacios(df, 'NombreVendedorDestino')

    # 3) Transformaciones de negocio
    almacenes_clean, pos = fun.limp_trans(df)     # limpieza + estandarización
    df_rank = fun.rank(almacenes_clean)           # selección top por ventana móvil
    df_total, _ = fun.tran2(df_rank, pos)         # union POS + asignación regiones

    # 4) Resumen con columnas jerárquicas (Año / Mes / PESOS-UND)
    pivot = fun.resumen_pivot(df_total)

    # 5) Escritura del Excel en carpeta RESULTADOS con timestamp
    out_dir = Path("RESULTADOS")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"datos_{ts}.xlsx"

    # IMPORTANTE:
    # - Se mantiene el índice (index=True) para conservar los niveles (Region/Vendedor/Tipo/Cliente).
    # - merge_cells=True para que Excel visualice correctamente los niveles de columnas (Año/Mes).
    pivot.to_excel(out_file, index=True, merge_cells=True)
    return out_file

# =========================
# Rutas Flask
# =========================
@app.get("/")
def index():
    """Renderiza la interfaz con el botón de ejecución/descarga."""
    return render_template_string(INDEX_HTML)

@app.post("/descargar")
def descargar():
    """
    Ejecuta el pipeline y envía el archivo generado al navegador.
    Nota: en disco se guarda con timestamp, pero se descarga SIEMPRE como 'datos.xlsx'
    para estandarizar el nombre entre usuarios.
    """
    try:
        out_file = ejecutar_pipeline()
        return send_file(out_file, as_attachment=True, download_name="datos.xlsx")
    except Exception as ex:
        # Log detallado en consola para soporte
        import traceback, sys
        print("\n===== ERROR /descargar =====", file=sys.stderr)
        traceback.print_exc()
        print("===== FIN ERROR =====\n", file=sys.stderr)
        return f"Error al ejecutar el proceso: {type(ex).__name__}: {ex}", 500

# =========================
# Arranque del servidor
# =========================
if __name__ == "__main__":
    # 0.0.0.0 permite acceder desde la red local (muestra también la IP en consola).
    app.run(host="0.0.0.0", port=5000, debug=True)
