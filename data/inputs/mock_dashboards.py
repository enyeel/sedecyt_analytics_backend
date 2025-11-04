MOCK_DASHBOARDS = [
  {
    "id": 'industrial-2024',
    "title": 'Resumen Industrial 2024',
    "description": 'Análisis del sector automotriz, aeroespacial y textil.',
    "imageUrl": 'https://placehold.co/400x200/003366/FFFFFF?text=Industrial',
    # Datos de gráficas (basado en tu test_data.json)
    "charts": [
      {
        "chart_id": "chart-001",
        "title": "Empresas por Rubro",
        "type": "bar",
        "data": {
          "labels": ["Automotriz", "Aeroespacial", "Alimentos", "Textil"],
          "datasets": [
            {
              "label": "Número de Empresas",
              "data": [120, 65, 80, 40],
              "backgroundColor": "rgba(54, 162, 235, 0.6)",
            },
          ],
        },
      },
      {
        "chart_id": "chart-002",
        "title": "Planes de Expansión",
        "type": "pie",
        "data": {
          "labels": ["Con Planes", "Sin Planes"],
          "datasets": [
            {
              "label": "Planes de Expansión",
              "data": [85, 175],
              "backgroundColor": ["rgba(75, 192, 192, 0.6)", "rgba(255, 99, 132, 0.6)"],
            },
          ],
        },
      },
      {
        "chart_id": "chart-003",
        "title": "Nuevos Empleos (Últimos 6 Meses)",
        "type": "line", # <-- Un tipo nuevo
        "data": {
          "labels": ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio"],
          "datasets": [
            {
              "label": "Nuevos Empleos",
              "data": [30, 45, 60, 50, 70, 85],
              "borderColor": "rgba(75, 192, 192, 1)",
              "tension": 0.1,
            },
          ],
        },
      },
    ]
  },
  {
    "id": 'comercio-2024',
    "title": 'Análisis de Comercio',
    "description": 'Reporte de importaciones y exportaciones por sector.',
    "imageUrl": 'https://placehold.co/400x200/556B2F/FFFFFF?text=Comercio',
    "charts": [] # Este no tiene gráficas por ahora
  },
  {
    "id": 'empleo-q3-2024',
    "title": 'Reporte de Empleo Q3',
    "description": 'Nuevos empleos generados, salarios promedio y vacantes.',
    "imageUrl": 'https://placehold.co/400x200/FF0066/FFFFFF?text=Empleo',
    "charts": [] # Este no tiene gráficas por ahora
  },
  {
    "id": 'companies-summary',
    "title": 'Análisis de Empresas',
    "description": 'Distribución de empresas registradas por sector y municipio.',
    "imageUrl": 'https://placehold.co/400x200/6A5ACD/FFFFFF?text=Empresas',
    "charts": [] # Se llenará dinámicamente desde el servicio
  }
]

#CURSO DE PROSPECTTIVA
#TOP 10 EMPRESAS QUE.. REPORTES
#PROPUESTAS DE DATOS Q PUEDEN SER RELEVANTES
#CUANTOS REPORTES SE CONTESTARON COMPLETOS O HASTA DONDE
#PEDIR RESPONDER ENCUESTA
#BUSQUEDA DE EMPRESAS
# REVISAR CONXION CON HUBSPOT