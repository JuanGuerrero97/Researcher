import asyncio
from typing import List

import pandas as pd
from aiohttp import ClientSession, TCPConnector


class DeudoresScraper:
    """
    Scraper asíncrono para consultar morosidad en Rama Judicial.
    Usa semáforo y connector para limitar concurrencia.
    """

    def __init__(self, url: str, max_concurrent: int) -> None:
        """
        Parameters
        ----------
        url : str
            Endpoint para POST.
        max_concurrent : int
            Máximo de peticiones concurrentes.
        """
        self.url = url
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.connector = TCPConnector(limit_per_host=max_concurrent)

    async def _fetch(self, session: ClientSession, doc: str) -> dict:
        """
        Hace POST y procesa respuesta de morosidad.

        Returns
        -------
        dict
            {'Documento': doc, 'Sancionado': ..., 'Estado': ...}.
        """
        payload = {"Documento": doc}
        try:
            async with session.post(self.url, json=payload, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    total = data.get("Total", 0)
                    items = data.get("Data", [])
                    if total and items:
                        sancionado = items[0].get("Sancionado")
                        estado = "Moroso"
                    else:
                        sancionado = None
                        estado = "No moroso"
                else:
                    sancionado = None
                    estado = "Error"
        except Exception:
            sancionado = None
            estado = "Error"

        return {"Documento": doc, "Sancionado": sancionado, "Estado": estado}

    async def run(
        self,
        nuips: List[str],
        progress_bar,
        progress_label,
    ) -> pd.DataFrame:
        """
        Ejecuta las consultas de morosidad y actualiza UI.

        Parameters
        ----------
        nuips : List[str]
        progress_bar : st.Progress
        progress_label : st.Empty

        Returns
        -------
        pd.DataFrame
        """
        async with ClientSession(connector=self.connector) as session:
            tasks = [
                self._limited_task(session, str(doc))
                for doc in nuips
            ]
            total = len(tasks)
            resultados = []
            for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
                resultados.append(await coro)
                fraction = idx / total
                progress_bar.progress(fraction)
                progress_label.text(f"{idx} de {total} ({fraction:.1%})")

        return pd.DataFrame(resultados)

    async def _limited_task(self, session, doc):
        async with self.semaphore:
            return await self._fetch(session, doc)
