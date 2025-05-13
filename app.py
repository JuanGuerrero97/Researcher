import asyncio
from time import perf_counter

import streamlit as st

from config.scrappers_config import SCRAPERS
from scrappers import SCRAPER_CLASSES
from utils.data_loader import load_data


def main():
    """Punto de entrada de la aplicación Streamlit."""
    st.set_page_config(page_title="KnowMe", layout="wide")
    st.title("📡 KnowMe")
    st.markdown(
        "Sube un archivo CSV o Excel con una sola columna de documentos "
        "para consultar en los diferentes servicios."
    )

    uploaded = st.file_uploader("Selecciona CSV o XLSX", type=["csv", "xlsx"])
    if not uploaded:
        st.info("Espera a que subas tu archivo.")
        return

    df = load_data(uploaded)
    if df.shape[1] != 1:
        st.error("El archivo debe tener exactamente UNA columna.")
        return

    nuips = df.iloc[:, 0].astype(str).tolist()

    with st.sidebar:
        st.header("Opciones")
        choice = st.selectbox("Selecciona scraper:", list(SCRAPERS.keys()))
        run_button = st.button("Iniciar Scraping")

    if not run_button:
        return

    cfg = SCRAPERS[choice]
    ScraperClass = SCRAPER_CLASSES.get(choice)
    if ScraperClass is None:
        st.error(f"No existe implementación para el scrapper '{choice}'.")
        return

    scraper = ScraperClass(**cfg)

    start = perf_counter()
    progress_bar = st.progress(0.0)
    progress_label = st.empty()

    with st.spinner("Ejecutando scraping..."):
        df_res = asyncio.run(scraper.run(nuips, progress_bar, progress_label))

    elapsed = perf_counter() - start
    progress_bar.empty()
    progress_label.empty()

    st.success(f"¡Completado en {elapsed:.1f} segundos!")
    st.dataframe(df_res)

    csv_bytes = df_res.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇ Descargar resultados",
        csv_bytes,
        file_name=f"resultados_{choice}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
