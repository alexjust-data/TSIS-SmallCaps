#!/usr/bin/env python3
"""
Script optimizado para descargar TODOS los datos fundamentales de Polygon/Massive.
Incluye: Balance Sheets, Income Statements, Cash Flow, Financial Ratios
"""

import asyncio
import aiohttp
import polars as pl
import json
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import backoff
import numpy as np

# ==========================================
# CÁLCULO DE RATIOS PARA SMALL CAPS
# ==========================================

def calculate_smallcap_ratios(balance_data: List[Dict], income_data: List[Dict],
                              cashflow_data: List[Dict]) -> List[Dict]:
    """
    Calcula ratios críticos para trading small caps:
    - Riesgo de bancarrota (liquidez)
    - Riesgo de dilución (burn rate)
    - Stress de deuda
    """
    if not balance_data:
        return []

    ratios_list = []

    # Indexar por periodo fiscal para hacer matching
    income_by_period = {d.get('fiscal_period'): d for d in income_data} if income_data else {}
    cashflow_by_period = {d.get('fiscal_period'): d for d in cashflow_data} if cashflow_data else {}

    for balance in balance_data:
        fiscal_period = balance.get('fiscal_period')
        income = income_by_period.get(fiscal_period, {})
        cashflow = cashflow_by_period.get(fiscal_period, {})

        ratios = {
            'ticker': balance.get('ticker'),
            'fiscal_period': fiscal_period,
            'fiscal_year': balance.get('fiscal_year'),
            'end_date': balance.get('end_date'),
        }

        # ===== RATIOS DE LIQUIDEZ (Bankruptcy Risk) =====
        current_assets = balance.get('current_assets', 0) or 0
        current_liabilities = balance.get('current_liabilities', 0) or 0
        inventory = balance.get('inventory', 0) or 0
        cash = balance.get('cash_and_cash_equivalents', 0) or balance.get('cash', 0) or 0

        # Current Ratio
        ratios['current_ratio'] = (current_assets / current_liabilities) if current_liabilities > 0 else None

        # Quick Ratio (Acid Test)
        ratios['quick_ratio'] = ((current_assets - inventory) / current_liabilities) if current_liabilities > 0 else None

        # Cash Ratio
        ratios['cash_ratio'] = (cash / current_liabilities) if current_liabilities > 0 else None

        # ===== BURN RATE (Dilution Risk) =====
        operating_cf = cashflow.get('operating_cash_flow', 0) or cashflow.get('net_cash_flow_from_operating_activities', 0) or 0

        # Quarterly Burn Rate (negativo = quemando cash)
        ratios['quarterly_burn_rate'] = -operating_cf if operating_cf < 0 else 0

        # Cash Runway (cuántos quarters de cash quedan)
        if ratios['quarterly_burn_rate'] > 0:
            ratios['cash_runway_quarters'] = cash / ratios['quarterly_burn_rate']
        else:
            ratios['cash_runway_quarters'] = None  # No está quemando cash

        # ===== DEBT STRESS =====
        total_debt = balance.get('total_debt', 0) or balance.get('long_term_debt', 0) or 0
        total_equity = balance.get('total_equity', 0) or balance.get('stockholders_equity', 0) or 0
        total_assets = balance.get('total_assets', 0) or 0

        # Debt to Equity
        ratios['debt_to_equity'] = (total_debt / total_equity) if total_equity > 0 else None

        # Debt to Assets
        ratios['debt_to_assets'] = (total_debt / total_assets) if total_assets > 0 else None

        # ===== PROFITABILITY =====
        revenue = income.get('revenues', 0) or income.get('total_revenue', 0) or 0
        gross_profit = income.get('gross_profit', 0) or 0
        operating_income = income.get('operating_income', 0) or 0
        net_income = income.get('net_income', 0) or income.get('net_income_loss', 0) or 0

        # Gross Margin
        ratios['gross_margin'] = (gross_profit / revenue) if revenue > 0 else None

        # Operating Margin
        ratios['operating_margin'] = (operating_income / revenue) if revenue > 0 else None

        # Net Margin
        ratios['net_margin'] = (net_income / revenue) if revenue > 0 else None

        # ROA
        ratios['roa'] = (net_income / total_assets) if total_assets > 0 else None

        # ROE
        ratios['roe'] = (net_income / total_equity) if total_equity > 0 else None

        # ===== FLAGS DE RIESGO =====
        # Flag: Alto riesgo de bancarrota
        ratios['bankruptcy_risk_flag'] = (
            (ratios['current_ratio'] is not None and ratios['current_ratio'] < 1.0) or
            (ratios['cash_runway_quarters'] is not None and ratios['cash_runway_quarters'] < 4)
        )

        # Flag: Alto riesgo de dilución
        ratios['dilution_risk_flag'] = (
            ratios['cash_runway_quarters'] is not None and
            ratios['cash_runway_quarters'] < 6
        )

        # Flag: Alto apalancamiento
        ratios['high_leverage_flag'] = (
            ratios['debt_to_equity'] is not None and ratios['debt_to_equity'] > 2.0
        )

        ratios_list.append(ratios)

    return ratios_list

# ==========================================
# CONFIGURACIÓN
# ==========================================

@dataclass
class FundamentalType:
    name: str
    endpoint: str
    params: Dict[str, Any]
    output_dir: str

FUNDAMENTAL_TYPES = [
    FundamentalType(
        name="balance_sheets",
        endpoint="/stocks/financials/v1/balance-sheets",
        params={"limit": 100},
        output_dir="balance_sheets"
    ),
    FundamentalType(
        name="income_statements",
        endpoint="/stocks/financials/v1/income-statements",
        params={"limit": 100},
        output_dir="income_statements"
    ),
    FundamentalType(
        name="cash_flow",
        endpoint="/stocks/financials/v1/cash-flow-statements",
        params={"limit": 100},
        output_dir="cash_flow_statements"
    ),
]

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

# ==========================================
# DESCARGADOR ASÍNCRONO
# ==========================================

class FundamentalsDownloader:
    def __init__(self, api_key: str, max_concurrent: int = 20):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.debug_first = True
        self.debug_ticker = None
        
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=5,
        max_time=60
    )
    async def fetch_fundamental(self, session: aiohttp.ClientSession,
                               ticker: str, fundamental_type: FundamentalType) -> Optional[Dict]:
        """Descarga datos fundamentales para un ticker"""

        url = f"{self.base_url}{fundamental_type.endpoint}"
        params = {
            "tickers": ticker,  # Nuevo API usa "tickers" (plural)
            "apiKey": self.api_key,
            **fundamental_type.params
        }

        async with self.semaphore:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    # Debug: mostrar status del primer request
                    if self.debug_first and ticker == self.debug_ticker:
                        log(f"DEBUG HTTP Status for {ticker}: {response.status}", "DEBUG")

                    if response.status == 404:
                        return None  # No data available

                    if response.status == 429:
                        retry_after = int(response.headers.get('X-Polygon-Retry-After', '5'))
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Rate limited")

                    if response.status == 403:
                        # Plan no tiene acceso a este endpoint
                        if self.debug_first and ticker == self.debug_ticker:
                            try:
                                error_data = await response.json()
                                log(f"DEBUG 403 Error: {error_data}", "DEBUG")
                            except:
                                pass
                        return None

                    response.raise_for_status()
                    data = await response.json()

                    # Debug: mostrar respuesta para primer ticker
                    if self.debug_first and ticker == self.debug_ticker:
                        log(f"DEBUG Response for {ticker}: status={data.get('status')}, "
                            f"results_count={len(data.get('results', []))}, "
                            f"request_id={data.get('request_id')}", "DEBUG")
                        self.debug_first = False

                    # Verificar si hay resultados
                    if data.get('results'):
                        return {
                            'ticker': ticker,
                            'type': fundamental_type.name,
                            'data': data['results']
                        }
                    return None

            except asyncio.TimeoutError:
                log(f"Timeout para {ticker} - {fundamental_type.name}", "WARNING")
                return None
            except aiohttp.ClientResponseError as e:
                if e.status != 403:  # No loguear errores 403 (plan restriction)
                    log(f"Error para {ticker} - {fundamental_type.name}: {e}", "ERROR")
                return None
            except Exception as e:
                log(f"Error para {ticker} - {fundamental_type.name}: {e}", "ERROR")
                return None
    
    async def download_all_fundamentals(self, tickers: List[str], output_dir: str,
                                       calculate_custom_ratios: bool = True):
        """Descarga todos los tipos de fundamentales para lista de tickers"""

        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        async with aiohttp.ClientSession(connector=connector) as session:

            # Almacenar datos por ticker para calcular ratios después
            ticker_data = {ticker: {} for ticker in tickers}

            for fundamental_type in FUNDAMENTAL_TYPES:
                log(f"\n{'='*60}")
                log(f"Descargando {fundamental_type.name}")
                log(f"Endpoint: {fundamental_type.endpoint}")
                log(f"{'='*60}")

                # Configurar debug para el primer ticker
                self.debug_first = True
                self.debug_ticker = tickers[0] if tickers else None

                output_path = Path(output_dir) / fundamental_type.output_dir
                output_path.mkdir(parents=True, exist_ok=True)

                # Procesar en batches
                batch_size = 100
                for i in range(0, len(tickers), batch_size):
                    batch = tickers[i:i + batch_size]

                    tasks = [
                        self.fetch_fundamental(session, ticker, fundamental_type)
                        for ticker in batch
                    ]

                    results = await asyncio.gather(*tasks)

                    # Guardar resultados
                    successful = 0
                    for result in results:
                        if result:
                            ticker = result['ticker']
                            data = result['data']

                            # Almacenar para cálculo de ratios
                            ticker_data[ticker][fundamental_type.name] = data

                            # Guardar como parquet por ticker
                            ticker_file = output_path / f"{ticker}.parquet"

                            try:
                                df = pl.DataFrame(data)
                                df.write_parquet(ticker_file)
                                successful += 1
                            except Exception as e:
                                log(f"Error guardando {ticker}: {e}", "ERROR")

                    log(f"Batch {i//batch_size + 1}: {successful}/{len(batch)} exitosos")

                log(f"Completado {fundamental_type.name}")

            # ===== CALCULAR RATIOS PERSONALIZADOS PARA SMALL CAPS =====
            if calculate_custom_ratios:
                log(f"\n{'='*60}")
                log("Calculando ratios personalizados para Small Caps")
                log(f"{'='*60}")

                ratios_path = Path(output_dir) / "smallcap_ratios"
                ratios_path.mkdir(parents=True, exist_ok=True)

                successful_ratios = 0
                for ticker in tickers:
                    data = ticker_data.get(ticker, {})

                    balance_data = data.get('balance_sheets', [])
                    income_data = data.get('income_statements', [])
                    cashflow_data = data.get('cash_flow', [])

                    if balance_data:
                        try:
                            ratios = calculate_smallcap_ratios(balance_data, income_data, cashflow_data)

                            if ratios:
                                df = pl.DataFrame(ratios)
                                df.write_parquet(ratios_path / f"{ticker}.parquet")
                                successful_ratios += 1
                        except Exception as e:
                            log(f"Error calculando ratios para {ticker}: {e}", "ERROR")

                log(f"Ratios calculados: {successful_ratios}/{len(tickers)} tickers")

# ==========================================
# DESCARGA DE SHORT INTEREST & VOLUME
# ==========================================

class ShortDataDownloader:
    def __init__(self, api_key: str, max_concurrent: int = 30):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.semaphore = asyncio.Semaphore(max_concurrent)

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=30
    )
    async def download_short_interest(self, session: aiohttp.ClientSession,
                                     ticker: str, date_from: str, date_to: str) -> Optional[pl.DataFrame]:
        """Descarga short interest histórico (reportado bi-semanalmente)"""

        # Endpoint correcto: /stocks/v1/short-interest
        url = f"{self.base_url}/stocks/v1/short-interest"
        params = {
            "ticker": ticker,
            "settlement_date.gte": date_from,
            "settlement_date.lte": date_to,
            "apiKey": self.api_key,
            "limit": 50000
        }

        async with self.semaphore:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 404:
                        return None
                    if response.status == 429:
                        await asyncio.sleep(5)
                        raise aiohttp.ClientError("Rate limited")
                    response.raise_for_status()
                    data = await response.json()

                    if data.get('results'):
                        df = pl.DataFrame(data['results'])
                        # Agregar métricas derivadas para ML
                        if 'short_volume' in df.columns and 'total_volume' in df.columns:
                            df = df.with_columns([
                                (pl.col('short_volume') / pl.col('total_volume')).alias('short_ratio'),
                            ])
                        return df
                    return None

            except Exception as e:
                log(f"Error short interest {ticker}: {e}", "ERROR")
                return None

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=30
    )
    async def download_short_volume(self, session: aiohttp.ClientSession,
                                   ticker: str, date_from: str, date_to: str) -> Optional[pl.DataFrame]:
        """
        Descarga short volume DIARIO para ML features.
        Incluye métricas derivadas útiles para trading.
        """

        # Endpoint correcto: /stocks/v1/short-volume
        url = f"{self.base_url}/stocks/v1/short-volume"

        all_results = []
        next_url = None

        async with self.semaphore:
            while True:
                try:
                    if next_url:
                        full_url = next_url if 'apiKey=' in next_url else f"{next_url}&apiKey={self.api_key}"
                        async with session.get(full_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            data = await response.json()
                    else:
                        params = {
                            "ticker": ticker,
                            "date.gte": date_from,
                            "date.lte": date_to,
                            "apiKey": self.api_key,
                            "limit": 50000  # Máximo para obtener todo el historial
                        }
                        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                            if response.status == 404:
                                return None
                            if response.status == 429:
                                await asyncio.sleep(5)
                                raise aiohttp.ClientError("Rate limited")
                            response.raise_for_status()
                            data = await response.json()

                    results = data.get('results', [])
                    if results:
                        all_results.extend(results)

                    next_url = data.get('next_url')
                    if not next_url:
                        break

                except Exception as e:
                    log(f"Error short volume {ticker}: {e}", "ERROR")
                    break

        if not all_results:
            return None

        df = pl.DataFrame(all_results)

        # ===== MÉTRICAS DERIVADAS PARA ML =====
        if 'short_volume' in df.columns and 'total_volume' in df.columns:
            df = df.with_columns([
                # Short ratio diario
                (pl.col('short_volume') / pl.col('total_volume')).alias('short_ratio'),

                # Exempt vs non-exempt short
                (pl.col('short_exempt_volume') / pl.col('short_volume')).fill_null(0).alias('exempt_ratio')
                if 'short_exempt_volume' in df.columns else pl.lit(0).alias('exempt_ratio'),
            ])

            # Agregar rolling metrics si hay suficientes datos
            if len(df) > 5:
                df = df.sort('date').with_columns([
                    # Media móvil 5 días del short ratio
                    pl.col('short_ratio').rolling_mean(window_size=5).alias('short_ratio_ma5'),

                    # Cambio vs día anterior
                    (pl.col('short_ratio') - pl.col('short_ratio').shift(1)).alias('short_ratio_change'),

                    # Z-score del short ratio (20 días)
                    ((pl.col('short_ratio') - pl.col('short_ratio').rolling_mean(window_size=20)) /
                     pl.col('short_ratio').rolling_std(window_size=20)).alias('short_ratio_zscore'),
                ])

        return df
    
    async def download_all_short_data(self, tickers: List[str], output_dir: str,
                                     date_from: str = "2004-01-01",
                                     date_to: Optional[str] = None):
        """
        Descarga todos los datos de short interest/volume.
        Optimizado para descargas masivas con métricas ML.
        """

        if not date_to:
            date_to = datetime.now().strftime("%Y-%m-%d")

        log(f"\n{'='*60}")
        log(f"Descargando Short Interest & Volume (con métricas ML)")
        log(f"Período: {date_from} a {date_to}")
        log(f"Tickers: {len(tickers):,}")
        log(f"{'='*60}")

        # Crear directorios
        short_interest_dir = Path(output_dir) / "short_interest"
        short_volume_dir = Path(output_dir) / "short_volume"
        short_interest_dir.mkdir(parents=True, exist_ok=True)
        short_volume_dir.mkdir(parents=True, exist_ok=True)

        connector = aiohttp.TCPConnector(limit=100, limit_per_host=50)
        async with aiohttp.ClientSession(connector=connector) as session:

            batch_size = 100
            total_batches = (len(tickers) + batch_size - 1) // batch_size

            si_success = 0
            sv_success = 0

            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i + batch_size]
                batch_num = i // batch_size + 1

                # ===== Short Interest (bi-semanal) =====
                si_tasks = [
                    self.download_short_interest(session, ticker, date_from, date_to)
                    for ticker in batch
                ]

                si_results = await asyncio.gather(*si_tasks, return_exceptions=True)

                for ticker, result in zip(batch, si_results):
                    if isinstance(result, pl.DataFrame) and not result.is_empty():
                        output_file = short_interest_dir / f"{ticker}.parquet"
                        result.write_parquet(output_file)
                        si_success += 1

                # ===== Short Volume (diario con métricas ML) =====
                sv_tasks = [
                    self.download_short_volume(session, ticker, date_from, date_to)
                    for ticker in batch
                ]

                sv_results = await asyncio.gather(*sv_tasks, return_exceptions=True)

                for ticker, result in zip(batch, sv_results):
                    if isinstance(result, pl.DataFrame) and not result.is_empty():
                        output_file = short_volume_dir / f"{ticker}.parquet"
                        result.write_parquet(output_file)
                        sv_success += 1

                log(f"Batch {batch_num}/{total_batches} | SI: {si_success} | SV: {sv_success}")

            log(f"\nResumen Short Data:")
            log(f"  Short Interest: {si_success}/{len(tickers)} tickers")
            log(f"  Short Volume: {sv_success}/{len(tickers)} tickers")

# ==========================================
# DESCARGA DE REFERENCE DATA
# ==========================================

async def download_reference_data(api_key: str, output_dir: str):
    """Descarga datos de referencia estáticos"""
    
    log(f"\n{'='*60}")
    log("Descargando Reference Data")
    log(f"{'='*60}")
    
    ref_dir = Path(output_dir) / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    
    base_url = "https://api.polygon.io"
    
    # Market Status Upcoming (incluye holidays próximos)
    log("Descargando Market Status Upcoming...")
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}/v1/marketstatus/upcoming"
        params = {"apiKey": api_key}

        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data:
                    df = pl.DataFrame(data)
                    df.write_parquet(ref_dir / "market_status_upcoming.parquet")
                    log(f"  Guardado: {len(df)} upcoming events")
            else:
                log(f"  Skip: HTTP {response.status}")
    
    # Exchanges
    log("Descargando Exchanges...")
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}/v3/reference/exchanges"
        params = {"apiKey": api_key}
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            df = pl.DataFrame(data.get('results', []))
            df.write_parquet(ref_dir / "exchanges.parquet")
            log(f"  Guardado: {len(df)} exchanges")
    
    # Condition Codes
    log("Descargando Condition Codes...")
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}/v3/reference/conditions"
        params = {"apiKey": api_key, "asset_class": "stocks"}
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            df = pl.DataFrame(data.get('results', []))
            df.write_parquet(ref_dir / "condition_codes.parquet")
            log(f"  Guardado: {len(df)} condition codes")
    
    # Ticker Types
    log("Descargando Ticker Types...")
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}/v3/reference/tickers/types"
        params = {"apiKey": api_key}
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            df = pl.DataFrame(data.get('results', []))
            df.write_parquet(ref_dir / "ticker_types.parquet")
            log(f"  Guardado: {len(df)} ticker types")

# ==========================================
# DESCARGA DE IPOs
# ==========================================

async def download_ipos(api_key: str, output_dir: str, date_from: str = "2004-01-01"):
    """Descarga historial de IPOs desde el endpoint oficial /vX/reference/ipos"""

    log(f"\n{'='*60}")
    log("Descargando IPOs")
    log(f"Período: desde {date_from}")
    log(f"{'='*60}")

    base_url = "https://api.polygon.io"
    url = f"{base_url}/vX/reference/ipos"

    ipo_dir = Path(output_dir) / "ipos"
    ipo_dir.mkdir(parents=True, exist_ok=True)

    all_ipos = []
    next_url = None
    page = 1

    async with aiohttp.ClientSession() as session:
        while True:
            if next_url:
                full_url = next_url if next_url.startswith('http') else f"{base_url}{next_url}"
                if 'apiKey=' not in full_url:
                    full_url += f"&apiKey={api_key}"
                async with session.get(full_url) as response:
                    data = await response.json()
            else:
                params = {
                    "apiKey": api_key,
                    "listing_date.gte": date_from,
                    "sort": "listing_date",
                    "order": "asc",
                    "limit": 1000
                }
                async with session.get(url, params=params) as response:
                    data = await response.json()

            results = data.get('results', [])
            if results:
                for ipo in results:
                    all_ipos.append({
                        'ticker': ipo.get('ticker', ''),
                        'name': ipo.get('issuer_name', ipo.get('name', '')),
                        'listing_date': ipo.get('listing_date', ''),
                        'ipo_status': ipo.get('ipo_status', ''),
                        'security_type': ipo.get('security_type', ''),
                        'primary_exchange': ipo.get('primary_exchange', ''),
                        'shares_offered': ipo.get('shares_offered', None),
                        'price_range_low': ipo.get('price_range_low', None),
                        'price_range_high': ipo.get('price_range_high', None),
                        'final_price': ipo.get('final_issue_price', ipo.get('offer_price', None)),
                        'total_offer_size': ipo.get('total_offer_size', None),
                        'us_code': ipo.get('us_code', ''),
                        'isin': ipo.get('isin', ''),
                        'currency': ipo.get('currency_code', ''),
                    })

            next_url = data.get('next_url')
            if not next_url:
                break

            page += 1
            if page % 10 == 0:
                log(f"  Página {page}, {len(all_ipos)} IPOs encontrados...")
    
    # Guardar
    if all_ipos:
        df = pl.DataFrame(all_ipos)
        df = df.sort('listing_date')
        df.write_parquet(ipo_dir / "all_ipos.parquet")
        log(f"  Total IPOs guardados: {len(df)}")

        # Estadísticas por año
        df_with_year = df.with_columns(
            pl.col('listing_date').str.slice(0, 4).alias('year')
        )
        yearly_stats = df_with_year.group_by('year').agg(
            pl.len().alias('count')
        ).sort('year')

        log("\n  IPOs por año:")
        for row in yearly_stats.iter_rows(named=True):
            log(f"    {row['year']}: {row['count']}")
    else:
        log("  No se encontraron IPOs")


# ==========================================
# DESCARGA DE NEWS
# ==========================================

async def download_news(api_key: str, tickers: List[str], output_dir: str,
                        date_from: str = "2004-01-01", concurrent: int = 20):
    """Descarga noticias para cada ticker desde /v2/reference/news"""

    log(f"\n{'='*60}")
    log("Descargando News")
    log(f"Período: desde {date_from}")
    log(f"Tickers: {len(tickers):,}")
    log(f"{'='*60}")

    base_url = "https://api.polygon.io"
    news_dir = Path(output_dir) / "news"
    news_dir.mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(concurrent)
    tickers_with_news = 0
    total_articles = 0

    async def fetch_ticker_news(session: aiohttp.ClientSession, ticker: str) -> List[Dict]:
        """Descarga todas las noticias de un ticker con paginación"""
        async with semaphore:
            all_news = []
            next_url = None

            while True:
                try:
                    if next_url:
                        full_url = next_url if next_url.startswith('http') else f"{base_url}{next_url}"
                        if 'apiKey=' not in full_url:
                            full_url += f"&apiKey={api_key}"
                        async with session.get(full_url) as response:
                            if response.status != 200:
                                break
                            data = await response.json()
                    else:
                        params = {
                            "ticker": ticker,
                            "published_utc.gte": date_from,
                            "sort": "published_utc",
                            "order": "asc",
                            "limit": 1000,
                            "apiKey": api_key
                        }
                        async with session.get(f"{base_url}/v2/reference/news", params=params) as response:
                            if response.status != 200:
                                break
                            data = await response.json()

                    results = data.get('results', [])
                    if not results:
                        break

                    for article in results:
                        all_news.append({
                            'ticker': ticker,
                            'article_id': article.get('id', ''),
                            'title': article.get('title', ''),
                            'description': article.get('description', ''),
                            'published_utc': article.get('published_utc', ''),
                            'article_url': article.get('article_url', ''),
                            'publisher_name': article.get('publisher', {}).get('name', ''),
                            'tickers_mentioned': ','.join(article.get('tickers', [])),
                            'keywords': ','.join(article.get('keywords', [])) if article.get('keywords') else '',
                        })

                    next_url = data.get('next_url')
                    if not next_url:
                        break

                except Exception:
                    break

            return all_news

    async with aiohttp.ClientSession() as session:
        batch_size = 100
        for batch_num in range(0, len(tickers), batch_size):
            batch = tickers[batch_num:batch_num + batch_size]
            tasks = [fetch_ticker_news(session, ticker) for ticker in batch]
            results = await asyncio.gather(*tasks)

            batch_articles = []
            for ticker_news in results:
                if ticker_news:
                    tickers_with_news += 1
                    total_articles += len(ticker_news)
                    batch_articles.extend(ticker_news)

            # Guardar batch
            if batch_articles:
                batch_df = pl.DataFrame(batch_articles)
                batch_file = news_dir / f"news_batch_{batch_num // batch_size:04d}.parquet"
                batch_df.write_parquet(batch_file)

            log(f"  Batch {batch_num // batch_size + 1}/{(len(tickers) + batch_size - 1) // batch_size} | "
                f"Tickers con news: {tickers_with_news} | Artículos: {total_articles:,}")

    log(f"\nResumen News:")
    log(f"  Tickers con noticias: {tickers_with_news}/{len(tickers)}")
    log(f"  Total artículos: {total_articles:,}")


# ==========================================
# DESCARGA DE ECONOMIC DATA
# ==========================================

async def download_economic_data(api_key: str, output_dir: str, date_from: str = "2004-01-01"):
    """Descarga datos económicos: Treasury Yields, Inflation"""

    log(f"\n{'='*60}")
    log("Descargando Economic Data")
    log(f"Período: desde {date_from}")
    log(f"{'='*60}")

    base_url = "https://api.polygon.io"
    econ_dir = Path(output_dir) / "economic"
    econ_dir.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        # 1. Treasury Yields
        log("  Descargando Treasury Yields...")
        all_yields = []
        next_url = None

        while True:
            if next_url:
                full_url = next_url if next_url.startswith('http') else f"{base_url}{next_url}"
                if 'apiKey=' not in full_url:
                    full_url += f"&apiKey={api_key}"
                async with session.get(full_url) as response:
                    data = await response.json()
            else:
                params = {
                    "date.gte": date_from,
                    "sort": "date",
                    "order": "asc",
                    "limit": 50000,
                    "apiKey": api_key
                }
                async with session.get(f"{base_url}/fed/v1/treasury-yields", params=params) as response:
                    if response.status != 200:
                        log(f"    Error Treasury Yields: {response.status}")
                        break
                    data = await response.json()

            results = data.get('results', [])
            if results:
                all_yields.extend(results)

            next_url = data.get('next_url')
            if not next_url:
                break

        if all_yields:
            df = pl.DataFrame(all_yields)
            df.write_parquet(econ_dir / "treasury_yields.parquet")
            log(f"    Guardado: {len(df):,} registros de Treasury Yields")

        # 2. Inflation (CPI)
        log("  Descargando Inflation data...")
        all_inflation = []
        next_url = None

        while True:
            if next_url:
                full_url = next_url if next_url.startswith('http') else f"{base_url}{next_url}"
                if 'apiKey=' not in full_url:
                    full_url += f"&apiKey={api_key}"
                async with session.get(full_url) as response:
                    data = await response.json()
            else:
                params = {
                    "date.gte": date_from,
                    "sort": "date",
                    "order": "asc",
                    "limit": 50000,
                    "apiKey": api_key
                }
                async with session.get(f"{base_url}/fed/v1/inflation", params=params) as response:
                    if response.status != 200:
                        log(f"    Error Inflation: {response.status}")
                        break
                    data = await response.json()

            results = data.get('results', [])
            if results:
                all_inflation.extend(results)

            next_url = data.get('next_url')
            if not next_url:
                break

        if all_inflation:
            df = pl.DataFrame(all_inflation)
            df.write_parquet(econ_dir / "inflation.parquet")
            log(f"    Guardado: {len(df):,} registros de Inflation")

        # 3. Inflation Expectations
        log("  Descargando Inflation Expectations...")
        all_expectations = []
        next_url = None

        while True:
            if next_url:
                full_url = next_url if next_url.startswith('http') else f"{base_url}{next_url}"
                if 'apiKey=' not in full_url:
                    full_url += f"&apiKey={api_key}"
                async with session.get(full_url) as response:
                    data = await response.json()
            else:
                params = {
                    "date.gte": date_from,
                    "sort": "date",
                    "order": "asc",
                    "limit": 50000,
                    "apiKey": api_key
                }
                async with session.get(f"{base_url}/fed/v1/inflation-expectations", params=params) as response:
                    if response.status != 200:
                        log(f"    Error Inflation Expectations: {response.status}")
                        break
                    data = await response.json()

            results = data.get('results', [])
            if results:
                all_expectations.extend(results)

            next_url = data.get('next_url')
            if not next_url:
                break

        if all_expectations:
            df = pl.DataFrame(all_expectations)
            df.write_parquet(econ_dir / "inflation_expectations.parquet")
            log(f"    Guardado: {len(df):,} registros de Inflation Expectations")

# ==========================================
# MAIN
# ==========================================

async def main_async():
    parser = argparse.ArgumentParser(description='Descarga completa de fundamentales y reference data')
    parser.add_argument('--api-key', help='Polygon API key')
    parser.add_argument('--tickers-file', help='Archivo con lista de tickers (uno por línea)')
    parser.add_argument('--output-dir', required=True, help='Directorio de salida')
    parser.add_argument('--data-types', nargs='+',
                       choices=['fundamentals', 'short', 'reference', 'ipos', 'news', 'economic', 'all'],
                       default=['all'],
                       help='Tipos de datos a descargar')
    parser.add_argument('--date-from', default='2004-01-01', help='Fecha inicial para datos históricos')
    parser.add_argument('--date-to', help='Fecha final (default: hoy)')
    parser.add_argument('--concurrent', type=int, default=20, help='Requests concurrentes')

    args = parser.parse_args()

    # API key
    api_key = args.api_key or os.getenv('POLYGON_API_KEY')
    if not api_key:
        log("ERROR: Necesitas --api-key o POLYGON_API_KEY", "ERROR")
        return

    # Cargar tickers
    tickers = []
    if args.tickers_file:
        with open(args.tickers_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        log(f"Cargados {len(tickers)} tickers")

    # Determinar qué descargar
    download_all = 'all' in args.data_types

    # Reference Data (no necesita tickers)
    if download_all or 'reference' in args.data_types:
        await download_reference_data(api_key, args.output_dir)

    # IPOs (no necesita tickers)
    if download_all or 'ipos' in args.data_types:
        await download_ipos(api_key, args.output_dir, args.date_from)

    # Economic Data (no necesita tickers)
    if download_all or 'economic' in args.data_types:
        await download_economic_data(api_key, args.output_dir, args.date_from)

    # Datos que necesitan tickers
    if tickers:
        # Fundamentals
        if download_all or 'fundamentals' in args.data_types:
            downloader = FundamentalsDownloader(api_key, args.concurrent)
            await downloader.download_all_fundamentals(tickers, args.output_dir)

        # Short Interest/Volume
        if download_all or 'short' in args.data_types:
            short_downloader = ShortDataDownloader(api_key)
            await short_downloader.download_all_short_data(
                tickers, args.output_dir, args.date_from, args.date_to
            )

        # News
        if download_all or 'news' in args.data_types:
            await download_news(api_key, tickers, args.output_dir, args.date_from, args.concurrent)

    log("\n" + "="*60)
    log("DESCARGA COMPLETADA")
    log("="*60)

def main():
    asyncio.run(main_async())

if __name__ == '__main__':
    main()
