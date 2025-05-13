import asyncio
from typing import List

import pandas as pd
from aiohttp import ClientSession

from utils.ip_generator import generate_ip


class DefuncionesScraper:
    """
    Scraper asíncrono para consultar vigencia de cédula en Registraduría.
    Usa semáforo para concurrencia y cambia IP cada cierto intervalo.
    """

    def __init__(self, url: str, max_concurrent: int, ip_interval: int) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint para POST.
        max_concurrent : int
            Máximo de peticiones concurrentes.
        ip_interval : int
            Cuántas consultas antes de generar nueva IP.
        """
        self.url = url
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.ip_interval = ip_interval

    async def _fetch(self, session: ClientSession, nuip: str, ip: str) -> dict:
        """
        Hace una consulta POST para un solo número de documento.

        Returns
        -------
        dict
            {'Documento': nuip, 'Vigencia': 'Si|No disponible|Error'}.
        """
        payload = {"nuip": nuip, "ip": ip}
        try:
            async with session.post(self.url, json=payload, timeout=10) as resp:
                data = await resp.json()
                vigencia = data.get("vigencia", "No disponible")
        except Exception:
            vigencia = "Error"
        return {"Documento": nuip, "Vigencia": vigencia}

    async def _limited_task(self, session: ClientSession, nuip: str, ip: str) -> dict:
        """
        Wrapper que aplica el semáforo para limitar concurrencia.
        """
        async with self.semaphore:
            return await self._fetch(session, nuip, ip)

    async def run(
        self,
        nuips: List[str],
        progress_bar,
        progress_label,
    ) -> pd.DataFrame:
        """
        Ejecuta todas las consultas y actualiza la UI de Streamlit.

        Parameters
        ----------
        nuips : List[str]
            Lista de documentos.
        progress_bar : st.Progress
        progress_label : st.Empty

        Returns
        -------
        pd.DataFrame
        """
        tasks = []
        async with ClientSession() as session:
            current_ip = generate_ip()
            for idx, nuip in enumerate(nuips):
                # Renueva IP cada ip_interval peticiones
                if idx % self.ip_interval == 0:
                    current_ip = generate_ip()
                tasks.append(self._limited_task(session, nuip, current_ip))

            total = len(tasks)
            resultados = []
            for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
                resultados.append(await coro)
                frac = idx / total
                progress_bar.progress(frac)
                progress_label.text(f"{idx} de {total} ({frac:.1%})")

        return pd.DataFrame(resultados)