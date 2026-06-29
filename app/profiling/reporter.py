import json
from pathlib import Path
from string import Template

from app.profiling.schemas.profiling_schema import ProfilingReport
from app.utils.logger import Logger


DIMENSION_DOCS: list[dict[str, str]] = [
    {
        "name": "accuracy",
        "title": "Exactitud",
        "description": (
            "Verifica que los valores agregados (como total_amount o driver_pay) "
            "sean exactamente la suma matematica de sus componentes "
            "(fare_amount, extra, mta_tax, tip_amount, tolls_amount, impuestos y recargos)."
        ),
        "metrics": [
            {
                "name": "total_amount_match_pct",
                "desc": "Porcentaje de filas donde total_amount coincide con la suma de sus componentes (tolerancia ±0.02).",
            },
            {
                "name": "total_amount_mismatch_count",
                "desc": "Numero absoluto de filas con discrepancia entre el total y la suma de componentes.",
            },
            {
                "name": "total_amount_max_diff",
                "desc": "Maxima diferencia absoluta encontrada entre el total reportado y el calculado.",
            },
        ],
        "categories": "yellow, green, fhvhv (fhv no aplica — sin campos de monto)",
    },
    {
        "name": "completeness",
        "title": "Completitud",
        "description": (
            "Mide el porcentaje de celdas con valores no nulos. Se contempla que "
            "ciertos campos tienen nulos permitidos por regla de negocio "
            "(ej. SR_Flag en FHV, ehail_fee en green, airport_fee en yellow)."
        ),
        "metrics": [
            {
                "name": "completeness_score",
                "desc": "Score global = 1 - (nulos_no_permitidos / celdas_totales).",
            },
            {
                "name": "null_pct_<columna>",
                "desc": "Porcentaje de nulos por cada columna del dataset.",
            },
        ],
        "categories": "Todas las categorias",
    },
    {
        "name": "consistency",
        "title": "Consistencia",
        "description": (
            "Detecta contradicciones logicas entre campos del mismo registro: "
            "fechas de pickup posteriores al dropoff, duraciones de viaje > 24h, "
            "distancias o cantidades de pasajeros negativas, y coherencia de tipos."
        ),
        "metrics": [
            {
                "name": "pickup_before_dropoff_pct",
                "desc": "Porcentaje de filas donde pickup_datetime < dropoff_datetime.",
            },
            {
                "name": "trip_duration_lt_24h_pct",
                "desc": "Porcentaje de filas con duracion de viaje menor a 24 horas.",
            },
            {
                "name": "trip_distance_non_negative_pct",
                "desc": "Porcentaje de filas con distancia de viaje >= 0.",
            },
            {
                "name": "passenger_count_non_negative_pct",
                "desc": "Porcentaje de filas con numero de pasajeros >= 0.",
            },
        ],
        "categories": "Todas las categorias",
    },
    {
        "name": "integrity",
        "title": "Integridad",
        "description": (
            "Verifica la integridad referencial de los identificadores de zona "
            "(PULocationID y DOLocationID) contra la tabla de referencia "
            "zone-lookup-table.parquet (LocationID 1–265)."
        ),
        "metrics": [
            {
                "name": "PULocationID_valid_pct",
                "desc": "Porcentaje de PULocationID que existen en la tabla de zonas.",
            },
            {
                "name": "DOLocationID_valid_pct",
                "desc": "Porcentaje de DOLocationID que existen en la tabla de zonas.",
            },
        ],
        "categories": "Todas las categorias (fhv usa PUlocationID/DOlocationID en minuscula)",
    },
    {
        "name": "reasonableness",
        "title": "Razonabilidad",
        "description": (
            "Detecta valores ilogicos o fuera de rangos plausibles: "
            "numero de pasajeros excesivo, distancias imposibles, tarifas negativas "
            "o desproporcionadas, recargos fuera de rango, etc."
        ),
        "metrics": [
            {
                "name": "passenger_count_in_range_pct",
                "desc": "Pasajeros entre 0 y 9.",
            },
            {
                "name": "trip_distance_in_range_pct",
                "desc": "Distancia entre 0 y 500 millas.",
            },
            {
                "name": "fare_amount_in_range_pct",
                "desc": "Tarifa entre -200 y 5000 (permite ajustes por devoluciones).",
            },
            {
                "name": "total_amount_in_range_pct",
                "desc": "Monto total entre -200 y 5000.",
            },
            {
                "name": "<campo>_in_range_pct",
                "desc": "Rangos especificos por campo y categoria (tolls, tips, recargos, etc.).",
            },
        ],
        "categories": "Todas las categorias con rangos definidos por categoria",
    },
    {
        "name": "timeliness",
        "title": "Oportunidad",
        "description": (
            "Verifica que los viajes pertenezcan al periodo (año-mes) indicado "
            "por el nombre del archivo. Los datasets se nombran como {year}-{month:02d} "
            "y los pickups deben caer dentro de ese mes calendario."
        ),
        "metrics": [
            {
                "name": "pickup_in_period_pct",
                "desc": "Porcentaje de filas cuyo pickup_datetime cae en el año-mes esperado.",
            },
        ],
        "categories": "Todas las categorias",
    },
    {
        "name": "uniqueness",
        "title": "Unicidad",
        "description": (
            "Detecta filas duplicadas segun una clave compuesta que identifica "
            "un viaje de manera univoca. La clave varia por categoria: "
            "para FHV/FHVHV se usa base + timestamps; para yellow/green se usa "
            "VendorID + timestamps + PULocationID como proxy."
        ),
        "metrics": [
            {
                "name": "unique_rows_pct",
                "desc": "Porcentaje de filas no duplicadas segun la clave compuesta.",
            },
        ],
        "categories": "Todas las categorias",
    },
    {
        "name": "validity",
        "title": "Validez",
        "description": (
            "Corrobora la validez de los datos contra el diccionario de datos "
            "en data/dicts. Verifica que todas las columnas existan en el diccionario "
            "y que los valores de campos enumerados (VendorID, RatecodeID, "
            "payment_type, store_and_fwd_flag, etc.) pertenezcan al conjunto permitido."
        ),
        "metrics": [
            {
                "name": "<campo>_valid_pct",
                "desc": "Porcentaje de valores validos para campos con enumeracion en el diccionario.",
            },
            {
                "name": "unknown_columns",
                "desc": "Lista de columnas presentes en el dataset pero ausentes en el diccionario.",
            },
        ],
        "categories": "Todas las categorias",
    },
]


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Profiling — NY TLC Trip Records</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f0f2f5; color: #1a1a2e; line-height: 1.5;
        }
        .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #fff; padding: 28px 20px; text-align: center;
        }
        .header h1 { font-size: 1.7em; margin-bottom: 4px; }
        .header p { color: #a0a0c0; font-size: 0.95em; }

        .tabs {
            display: flex; justify-content: center; gap: 4px;
            background: #16213e; padding: 0 20px; flex-wrap: wrap;
        }
        .tab-btn {
            background: transparent; color: #a0a0c0; border: none;
            padding: 12px 28px; cursor: pointer; font-size: 0.95em;
            font-weight: 500; border-bottom: 3px solid transparent;
            transition: all 0.2s;
        }
        .tab-btn:hover { color: #fff; background: rgba(255,255,255,0.05); }
        .tab-btn.active { color: #fff; border-bottom-color: #4CAF50; }

        .tab-content { display: none; padding: 24px 20px; max-width: 1200px; margin: 0 auto; }
        .tab-content.active { display: block; }

        .summary-boxes { display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
        .summary-box {
            flex: 1; min-width: 160px; background: #fff; border-radius: 10px;
            padding: 18px; text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        }
        .summary-box .num { font-size: 2.2em; font-weight: 700; }
        .summary-box .label { font-size: 0.82em; color: #777; margin-top: 4px; }
        .num.green { color: #4CAF50; }
        .num.red { color: #F44336; }
        .num.amber { color: #FF9800; }
        .num.blue { color: #2196F3; }

        .filter-bar {
            display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap;
            align-items: center;
        }
        .filter-bar select, .filter-bar input {
            padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px;
            font-size: 0.9em; background: #fff;
        }
        .filter-bar label { font-size: 0.85em; color: #666; font-weight: 500; }

        .dataset-card {
            background: #fff; border-radius: 10px; margin-bottom: 14px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.08); overflow: hidden;
            border-left: 4px solid #ccc;
        }
        .dataset-card.pass { border-left-color: #4CAF50; }
        .dataset-card.fail { border-left-color: #F44336; }

        .dataset-header {
            cursor: pointer; padding: 14px 18px; display: flex;
            align-items: center; gap: 14px; flex-wrap: wrap;
        }
        .dataset-header:hover { background: #fafbfc; }
        .dataset-name { font-weight: 700; font-size: 1.05em; }
        .badge {
            background: #e8eaf6; color: #3949ab; padding: 3px 10px;
            border-radius: 12px; font-size: 0.82em;
        }
        .date-range { color: #888; font-size: 0.82em; }
        .score-pill {
            margin-left: auto; font-weight: 700; font-size: 1.05em;
            padding: 4px 12px; border-radius: 16px; min-width: 70px; text-align: center;
        }
        .score-pill.green { background: #e8f5e9; color: #2e7d32; }
        .score-pill.red { background: #ffebee; color: #c62828; }
        .score-pill.amber { background: #fff3e0; color: #e65100; }

        .dataset-body { display: none; padding: 0 18px 14px; }
        .dataset-card.expanded .dataset-body { display: block; }

        table { width: 100%; border-collapse: collapse; margin-top: 8px; }
        th, td { padding: 9px 12px; text-align: left; border-bottom: 1px solid #f0f0f0; font-size: 0.88em; }
        th { background: #fafbfc; font-weight: 600; color: #555; font-size: 0.78em; text-transform: uppercase; letter-spacing: 0.03em; }
        td.dim-name { font-weight: 600; }
        td.status { font-weight: 600; }
        .status-pass { color: #4CAF50; }
        .status-fail { color: #F44336; }

        .bar-bg { width: 120px; height: 8px; background: #e0e0e0; border-radius: 4px; overflow: hidden; }
        .bar-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }

        .failed-metrics {
            margin-top: 10px; background: #fff8f8; border: 1px solid #ffcdd2;
            border-radius: 8px; padding: 12px;
        }
        .failed-metrics-title { font-size: 0.85em; font-weight: 600; color: #c62828; margin-bottom: 8px; }
        .failed-metric { font-size: 0.82em; margin-bottom: 6px; padding-left: 12px; border-left: 2px solid #F44336; }
        .failed-metric .mname { font-weight: 600; }
        .failed-metric .mval { color: #666; }
        .failed-metric .mdetail { color: #888; font-size: 0.95em; }

        .dim-doc-card {
            background: #fff; border-radius: 10px; margin-bottom: 16px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.08); overflow: hidden;
        }
        .dim-doc-header {
            padding: 16px 18px; background: #f5f7fa; border-left: 4px solid #2196F3;
        }
        .dim-doc-header h3 { font-size: 1.1em; margin-bottom: 4px; }
        .dim-doc-header .dim-code { font-size: 0.8em; color: #888; font-family: monospace; }
        .dim-doc-body { padding: 14px 18px; }
        .dim-doc-body p { color: #444; margin-bottom: 12px; font-size: 0.9em; }
        .dim-doc-body .categories { font-size: 0.8em; color: #888; margin-bottom: 12px; }
        .metric-list { list-style: none; }
        .metric-list li {
            padding: 8px 0; border-bottom: 1px solid #f5f5f5; font-size: 0.88em;
        }
        .metric-list li:last-child { border-bottom: none; }
        .metric-list .mname { font-family: monospace; font-weight: 600; color: #2196F3; }
        .metric-list .mdesc { color: #666; margin-top: 2px; }

        footer { text-align: center; padding: 24px; color: #aaa; font-size: 0.85em; }

        .arrow { margin-left: auto; transition: transform 0.2s; color: #aaa; }
        .dataset-card.expanded .arrow { transform: rotate(90deg); }
    </style>
</head>
<body>
    <div class="header">
        <h1>Perfilado de Datos — NY TLC Trip Records</h1>
        <p>Evaluacion de calidad sobre 8 dimensiones</p>
    </div>

    <div class="tabs">
        <button class="tab-btn active" onclick="switchTab('reports')">Reportes por Dataset</button>
        <button class="tab-btn" onclick="switchTab('docs')">Metricas Evaluadas</button>
    </div>

    <div id="tab-reports" class="tab-content active">
        <div class="summary-boxes">
            <div class="summary-box"><div class="num blue">$total</div><div class="label">Datasets evaluados</div></div>
            <div class="summary-box"><div class="num green">$passed</div><div class="label">Datasets aprobados</div></div>
            <div class="summary-box"><div class="num red">$failed</div><div class="label">Datasets con fallos</div></div>
            <div class="summary-box"><div class="num amber">$avg_score</div><div class="label">Score promedio</div></div>
        </div>

        <div class="filter-bar">
            <label>Categoria:</label>
            <select id="cat-filter" onchange="filterDatasets()">
                <option value="">Todas</option>
                $category_options
            </select>
            <label>Estado:</label>
            <select id="status-filter" onchange="filterDatasets()">
                <option value="">Todos</option>
                <option value="pass">Aprobados</option>
                <option value="fail">Con fallos</option>
            </select>
            <label>Buscar:</label>
            <input type="text" id="search-filter" placeholder="Nombre del dataset..." oninput="filterDatasets()">
        </div>

        <div id="dataset-list">$dataset_cards</div>
    </div>

    <div id="tab-docs" class="tab-content">
        <h2 style="margin-bottom:16px; font-size:1.3em;">Dimensiones y Metricas Evaluadas</h2>
        <p style="color:#666; margin-bottom:20px; font-size:0.9em;">
            A continuacion se describen las 8 dimensiones de calidad de datos evaluadas,
            las metricas calculadas para cada una y los criterios de aprobacion.
        </p>
        $dimension_docs
    </div>

    <footer>Generado por <code>app.profiling</code> — $total reportes</footer>

    <script>
        function switchTab(tabName) {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            event.target.classList.add('active');
            document.getElementById('tab-' + tabName).classList.add('active');
        }

        function toggleCard(card) {
            card.classList.toggle('expanded');
        }

        function filterDatasets() {
            const cat = document.getElementById('cat-filter').value;
            const status = document.getElementById('status-filter').value;
            const search = document.getElementById('search-filter').value.toLowerCase();
            document.querySelectorAll('.dataset-card').forEach(card => {
                const cardCat = card.dataset.category;
                const cardStatus = card.dataset.status;
                const cardName = card.dataset.name.toLowerCase();
                const catOk = !cat || cardCat === cat;
                const statusOk = !status || cardStatus === status;
                const searchOk = !search || cardName.includes(search);
                card.style.display = (catOk && statusOk && searchOk) ? '' : 'none';
            });
        }
    </script>
</body>
</html>"""


class Reporter:
    def __init__(self, output_dir: str = "data/profiling") -> None:
        self.output_dir = Path(output_dir)
        self.logger = Logger()

    def write_json(self, report: ProfilingReport) -> Path:
        cat_dir = self.output_dir / report.meta.category
        cat_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{report.meta.year}-{report.meta.month:02d}.json"
        filepath = cat_dir / filename

        data = report.model_dump(mode="json")
        filepath.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        self.logger.info(f"Reporte JSON guardado: {filepath}")
        return filepath

    def build_index_html(self, reports: list[ProfilingReport]) -> Path:
        filepath = self.output_dir / "index.html"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        reports_sorted = sorted(
            reports, key=lambda r: (r.meta.category, r.meta.year, r.meta.month)
        )

        total = len(reports)
        passed_count = sum(
            1 for r in reports if all(d.passed for d in r.dimensions)
        )
        failed_count = total - passed_count
        avg_score = (
            round(sum(r.overall_score for r in reports) / max(total, 1) * 100, 1)
            if total
            else 0
        )

        categories = sorted(set(r.meta.category for r in reports))
        category_options = "".join(
            f'<option value="{c}">{c}</option>' for c in categories
        )

        dataset_cards = "\n".join(
            self._render_dataset_card(r) for r in reports_sorted
        )
        dimension_docs = "\n".join(
            self._render_dimension_doc(doc) for doc in DIMENSION_DOCS
        )

        html = Template(HTML_TEMPLATE).substitute(
            total=total,
            passed=passed_count,
            failed=failed_count,
            avg_score=f"{avg_score}%",
            category_options=category_options,
            dataset_cards=dataset_cards,
            dimension_docs=dimension_docs,
        )

        filepath.write_text(html, encoding="utf-8")
        self.logger.info(f"Indice HTML generado: {filepath}")
        return filepath

    def _render_dataset_card(self, report: ProfilingReport) -> str:
        meta = report.meta
        ts = meta.time_span if meta.time_span else ("N/A", "N/A")
        all_passed = all(d.passed for d in report.dimensions)
        status_class = "pass" if all_passed else "fail"
        score_class = (
            "green" if report.overall_score >= 0.95
            else "amber" if report.overall_score >= 0.80
            else "red"
        )
        score_pct = f"{report.overall_score:.1%}"

        dims_html = "\n".join(
            self._render_dimension_row(dim) for dim in report.dimensions
        )

        failed_dims = [d for d in report.dimensions if not d.passed]
        failed_metrics_html = ""
        if failed_dims:
            failed_metrics_rows = []
            for dim in failed_dims:
                failed_only = [
                    m for m in dim.metrics
                    if isinstance(m.passed, bool) and not m.passed
                ]
                for m in failed_only:
                    val_str = self._format_value(m.value)
                    detail_str = self._format_detail(m.detail)
                    failed_metrics_rows.append(
                        f'<div class="failed-metric">'
                        f'<span class="mname">{dim.dimension}/{m.name}</span> '
                        f'<span class="mval">= {val_str}</span>'
                        f'{detail_str}</div>'
                    )
            if failed_metrics_rows:
                failed_metrics_html = (
                    '<div class="failed-metrics">'
                    '<div class="failed-metrics-title">Metricas fallidas</div>'
                    f'{"".join(failed_metrics_rows)}'
                    '</div>'
                )

        return f"""<div class="dataset-card {status_class}" data-category="{meta.category}" data-status="{status_class}" data-name="{meta.name}">
            <div class="dataset-header" onclick="toggleCard(this.parentElement)">
                <span class="dataset-name">{meta.name}</span>
                <span class="badge">{meta.rowcount:,} registros</span>
                <span class="date-range">{ts[0][:10]} → {ts[1][:10]}</span>
                <span class="score-pill {score_class}">{score_pct}</span>
                <span class="arrow">▶</span>
            </div>
            <div class="dataset-body">
                <table>
                    <thead><tr>
                        <th>Dimension</th><th>Score</th><th>Barra</th><th>Estado</th>
                    </tr></thead>
                    <tbody>{dims_html}</tbody>
                </table>
                {failed_metrics_html}
            </div>
        </div>"""

    def _render_dimension_row(self, dim) -> str:
        score_pct = f"{dim.score:.1%}"
        bar_width = min(dim.score * 100, 100)
        color = "#4CAF50" if dim.passed else "#F44336"
        status_text = "APROBADA" if dim.passed else "FALLIDA"
        status_class = "status-pass" if dim.passed else "status-fail"

        return f"""<tr>
            <td class="dim-name">{dim.dimension}</td>
            <td>{score_pct}</td>
            <td><div class="bar-bg"><div class="bar-fill" style="width:{bar_width:.0f}%;background:{color}"></div></div></td>
            <td class="status {status_class}">{status_text}</td>
        </tr>"""

    def _render_dimension_doc(self, doc: dict) -> str:
        metrics_html = "".join(
            f'<li><span class="mname">{m["name"]}</span><div class="mdesc">{m["desc"]}</div></li>'
            for m in doc["metrics"]
        )
        return f"""<div class="dim-doc-card">
            <div class="dim-doc-header">
                <h3>{doc["title"]}</h3>
                <span class="dim-code">{doc["name"]}</span>
            </div>
            <div class="dim-doc-body">
                <p>{doc["description"]}</p>
                <p class="categories"><strong>Categorias:</strong> {doc["categories"]}</p>
                <ul class="metric-list">{metrics_html}</ul>
            </div>
        </div>"""

    def _format_value(self, value) -> str:
        if isinstance(value, float):
            return f"{value:.4f}"
        if isinstance(value, list):
            return ", ".join(str(v) for v in value[:5])
        return str(value)

    def _format_detail(self, detail: dict) -> str:
        if not detail:
            return ""
        parts = []
        for k, v in detail.items():
            if isinstance(v, (list, dict)):
                continue
            parts.append(f"{k}={v}")
        if not parts:
            return ""
        return f'<div class="mdetail">{" | ".join(parts[:4])}</div>'
