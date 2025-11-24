#!/usr/bin/env python3
"""
Identifica días de alto volumen/eventos para descargar quotes selectivamente.
En lugar de descargar 5.5M de días, descarga solo ~500K días importantes.
"""

import polars as pl
import numpy as np
from pathlib import Path
import argparse
from datetime import datetime
from typing import Dict, List, Set

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

class SmartQuotesSelector:
    def __init__(self, daily_root: Path, output_dir: Path):
        self.daily_root = daily_root
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def identify_high_volume_days(self, ticker: str, year_min: int, year_max: int) -> List[Dict]:
        """
        Identifica días con volumen anormal (>2x promedio).
        Estos días típicamente tienen mayor actividad de quotes.
        """
        ticker_path = self.daily_root / ticker
        if not ticker_path.exists():
            return []
        
        high_volume_days = []
        
        for year in range(year_min, year_max + 1):
            year_file = ticker_path / f"year={year}" / "daily.parquet"
            if not year_file.exists():
                continue
                
            try:
                df = pl.read_parquet(year_file)

                # La columna puede llamarse 'volume' o 'v'
                vol_col = 'volume' if 'volume' in df.columns else 'v'

                # Calcular estadísticas de volumen
                vol_mean = df[vol_col].mean()
                vol_std = df[vol_col].std()
                vol_90p = df[vol_col].quantile(0.90)
                
                # Criterios para día de alto volumen
                df_analysis = df.with_columns([
                    # Volumen > 2x promedio
                    (pl.col(vol_col) > vol_mean * 2).alias('high_volume'),
                    # Volumen > percentil 90
                    (pl.col(vol_col) > vol_90p).alias('top_decile'),
                    # Gap > 5% (columnas pueden ser 'open' o 'o', 'close' o 'c', etc.)
                    ((pl.col('o') - pl.col('c').shift(1)) / pl.col('c').shift(1) * 100).abs().alias('gap_pct'),
                    # Rango intraday > 5%
                    ((pl.col('h') - pl.col('l')) / pl.col('o') * 100).alias('range_pct'),
                    # Cambio diario > 10%
                    ((pl.col('c') - pl.col('o')) / pl.col('o') * 100).abs().alias('change_pct')
                ])
                
                # Filtrar días importantes
                important_days = df_analysis.filter(
                    (pl.col('high_volume') == True) |
                    (pl.col('gap_pct') > 5) |
                    (pl.col('range_pct') > 5) |
                    (pl.col('change_pct') > 10)
                )
                
                for row in important_days.iter_rows(named=True):
                    high_volume_days.append({
                        'ticker': ticker,
                        'date': row['date'],
                        'volume': row[vol_col],
                        'volume_ratio': row[vol_col] / vol_mean if vol_mean > 0 else 0,
                        'gap_pct': row.get('gap_pct', 0),
                        'range_pct': row['range_pct'],
                        'change_pct': row['change_pct'],
                        'priority_score': self._calculate_priority(row, vol_mean)
                    })
                    
            except Exception as e:
                continue
                
        return high_volume_days
    
    def _calculate_priority(self, row: Dict, vol_mean: float) -> float:
        """
        Calcula score de prioridad para determinar qué días descargar primero.
        """
        score = 0.0
        
        # Peso por volumen relativo
        if vol_mean > 0:
            vol_ratio = row['volume'] / vol_mean
            score += min(vol_ratio, 10) * 10  # Max 100 puntos por volumen
        
        # Peso por gap
        if 'gap_pct' in row and row['gap_pct']:
            score += min(abs(row['gap_pct']), 20) * 5  # Max 100 puntos por gap
        
        # Peso por rango intraday
        score += min(row['range_pct'], 20) * 5  # Max 100 puntos por rango
        
        # Peso por cambio absoluto
        score += min(abs(row['change_pct']), 30) * 3  # Max 90 puntos por cambio
        
        return score
    
    def identify_event_days(self, ticker: str) -> Set[str]:
        """
        Identifica días con eventos específicos:
        - Días con splits/dividendos
        - Días alrededor de earnings (si tuvieras los datos)
        - Primer y último día del mes (rebalanceos)
        - Días de vencimiento de opciones (3er viernes)
        """
        event_days = set()
        
        # Cargar splits si existen
        splits_file = Path("raw/polygon/reference/splits") / f"{ticker}.parquet"
        if splits_file.exists():
            try:
                splits_df = pl.read_parquet(splits_file)
                if 'execution_date' in splits_df.columns:
                    event_days.update(splits_df['execution_date'].to_list())
            except:
                pass
        
        # Cargar dividendos si existen
        divs_file = Path("raw/polygon/reference/dividends") / f"{ticker}.parquet"
        if divs_file.exists():
            try:
                divs_df = pl.read_parquet(divs_file)
                if 'ex_dividend_date' in divs_df.columns:
                    event_days.update(divs_df['ex_dividend_date'].to_list())
            except:
                pass
        
        return event_days
    
    def generate_smart_quotes_list(self, 
                                  tickers: List[str],
                                  year_min: int,
                                  year_max: int,
                                  max_days_per_ticker: int = 200) -> pl.DataFrame:
        """
        Genera lista inteligente de días para descargar quotes.
        """
        all_priority_days = []
        
        for i, ticker in enumerate(tickers):
            if (i + 1) % 100 == 0:
                log(f"Procesando: {i+1}/{len(tickers)} tickers")
            
            # Identificar días de alto volumen
            high_vol_days = self.identify_high_volume_days(ticker, year_min, year_max)
            
            # Identificar días con eventos
            event_days = self.identify_event_days(ticker)
            
            # Combinar y priorizar
            ticker_days = high_vol_days
            
            # Agregar días de eventos
            for event_date in event_days:
                # Verificar si ya está en la lista
                if not any(d['date'] == event_date for d in ticker_days):
                    ticker_days.append({
                        'ticker': ticker,
                        'date': event_date,
                        'priority_score': 500  # Alta prioridad para eventos
                    })
            
            # Ordenar por prioridad y tomar top N
            ticker_days.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
            ticker_days = ticker_days[:max_days_per_ticker]
            
            all_priority_days.extend(ticker_days)
        
        # Convertir a DataFrame
        if all_priority_days:
            df = pl.DataFrame(all_priority_days)
            return df.sort(['priority_score', 'ticker', 'date'], descending=[True, False, False])
        
        return pl.DataFrame()
    
    def analyze_coverage(self, smart_df: pl.DataFrame, total_possible_days: int):
        """
        Analiza qué porcentaje de días importantes cubrimos.
        """
        log("\n" + "="*60)
        log("ANÁLISIS DE COBERTURA")
        log("="*60)
        
        unique_tickers = smart_df['ticker'].n_unique()
        total_selected = len(smart_df)
        avg_per_ticker = total_selected / unique_tickers if unique_tickers > 0 else 0
        
        log(f"Tickers únicos: {unique_tickers:,}")
        log(f"Días seleccionados: {total_selected:,}")
        log(f"Días totales posibles: {total_possible_days:,}")
        log(f"Cobertura: {total_selected/total_possible_days*100:.1f}%")
        log(f"Promedio días/ticker: {avg_per_ticker:.0f}")
        
        # Distribución por score
        if 'priority_score' in smart_df.columns:
            log("\nDistribución por prioridad:")
            score_bins = [0, 100, 200, 300, 400, 500, 1000]
            for i in range(len(score_bins)-1):
                count = len(smart_df.filter(
                    (pl.col('priority_score') >= score_bins[i]) &
                    (pl.col('priority_score') < score_bins[i+1])
                ))
                log(f"  Score {score_bins[i]}-{score_bins[i+1]}: {count:,} días")
        
        # Estimación de tamaño
        avg_size_per_day = 100  # KB promedio por día
        estimated_size_gb = (total_selected * avg_size_per_day) / (1024 * 1024)
        log(f"\nTamaño estimado: {estimated_size_gb:.1f} GB")
        log(f"vs. Tamaño total: {(total_possible_days * avg_size_per_day) / (1024 * 1024):.1f} GB")
        log(f"Ahorro: {(1 - total_selected/total_possible_days)*100:.1f}%")

def main():
    parser = argparse.ArgumentParser(description='Identifica días prioritarios para descargar quotes')
    parser.add_argument('--daily-root', required=True, help='Path to daily OHLCV')
    parser.add_argument('--ping-file', required=True, help='Path to ping_range parquet')
    parser.add_argument('--output-csv', required=True, help='Output CSV file')
    parser.add_argument('--year-min', type=int, required=True, help='Start year')
    parser.add_argument('--year-max', type=int, required=True, help='End year')
    parser.add_argument('--max-days-per-ticker', type=int, default=200,
                       help='Max días por ticker (default: 200)')
    parser.add_argument('--min-priority-score', type=float, default=50,
                       help='Score mínimo para incluir (default: 50)')
    
    args = parser.parse_args()
    
    log("="*60)
    log("SELECTOR INTELIGENTE DE QUOTES")
    log("="*60)
    
    # Cargar tickers
    log(f"Cargando tickers desde {args.ping_file}")
    ping_df = pl.read_parquet(args.ping_file)
    tickers = ping_df.filter(pl.col('has_data') == True)['ticker'].to_list()
    log(f"Tickers con datos: {len(tickers):,}")
    
    # Calcular días totales posibles (para comparación)
    # Aproximadamente 252 días trading por año
    years = args.year_max - args.year_min + 1
    total_possible_days = len(tickers) * years * 252
    log(f"Días totales posibles: {total_possible_days:,}")
    
    # Generar lista inteligente
    selector = SmartQuotesSelector(
        Path(args.daily_root),
        Path(args.output_csv).parent
    )
    
    log("\nIdentificando días de alto volumen y eventos...")
    smart_df = selector.generate_smart_quotes_list(
        tickers,
        args.year_min,
        args.year_max,
        args.max_days_per_ticker
    )
    
    # Filtrar por score mínimo
    if 'priority_score' in smart_df.columns:
        smart_df = smart_df.filter(pl.col('priority_score') >= args.min_priority_score)
    
    # Análisis de cobertura
    selector.analyze_coverage(smart_df, total_possible_days)
    
    # Guardar resultado
    output_df = smart_df.select(['ticker', 'date'])
    output_df.write_csv(args.output_csv)
    log(f"\nGuardado en: {args.output_csv}")
    
    # Guardar versión con metadata para análisis
    metadata_csv = args.output_csv.replace('.csv', '_with_metadata.csv')
    smart_df.write_csv(metadata_csv)
    log(f"Metadata guardada en: {metadata_csv}")

if __name__ == '__main__':
    main()
