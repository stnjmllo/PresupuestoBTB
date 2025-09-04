# app.py
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template_string, send_file
import a_funciones as fun

app = Flask(__name__)

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
      display:block;
      margin:28px auto 8px;
      max-width:660px;
      width:85vw;
      height:auto;
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
  <img src="{{ url_for('static', filename='VIVELL MELINA (1).png') }}" alt="Vivell Logo" class="logo-vivell">

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

  <script>
    const runBtn  = document.getElementById('runBtn');
    const spinner = document.getElementById('spinner');
    const runText = document.getElementById('runText');
    const msg     = document.getElementById('msg');

    async function descargar() {
      runBtn.disabled = true;
      spinner.classList.remove('d-none');
      runText.textContent = 'Procesando...';
      msg.textContent = '';

      try {
        const resp = await fetch('/descargar', { method: 'POST' });
        if (!resp.ok) {
          const text = await resp.text();
          throw new Error(text || ('HTTP ' + resp.status));
        }
        const dispo = resp.headers.get('Content-Disposition') || '';
        let filename = 'archivo.xlsx';
        const match = /filename\\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?/i.exec(dispo);
        if (match) filename = decodeURIComponent((match[1] || match[2] || 'archivo.xlsx'));

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

def ejecutar_pipeline():
    # 1) Extraer
    df = fun.extraer_datos()
    df = fun.limpiar_espacios(df, 'NombreVendedorDestino')
   
    almacenes_clean, pos = fun.limp_trans(df)
    df_rank = fun.rank(almacenes_clean)
    df_total, _ = fun.tran2(df_rank, pos)
   
    pivot = fun.resumen_pivot(df_total)

    
    out_dir = Path("RESULTADOS")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"datos_{ts}.xlsx"

   
    pivot.to_excel(out_file, index=True, merge_cells=True)
    return out_file

@app.get("/")
def index():
    return render_template_string(INDEX_HTML)

@app.post("/descargar")
def descargar():
    try:
        out_file = ejecutar_pipeline()
        
        return send_file(out_file, as_attachment=True, download_name="datos.xlsx")
    except Exception as ex:
        import traceback, sys
        print("\n===== ERROR /descargar =====", file=sys.stderr)
        traceback.print_exc()
        print("===== FIN ERROR =====\n", file=sys.stderr)
        return f"Error al ejecutar el proceso: {type(ex).__name__}: {ex}", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
