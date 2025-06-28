"""
Script para executar backtests de estratégias baseadas em arquivos JSON
gerados pelo strategy_generator.py
"""

import os
import json
import pandas as pd
from datetime import datetime
from futures_backtester import Backtester
import entries
import glob

# Dicionários de configuração (ajuste conforme necessário)
path_b3 = 'C:/Users/User/OneDrive/Documentos/rnt/Finance/Trading Projects/00.database/candlestick data/futuros/'
path_tickmill = 'C:/Users/User/OneDrive/Documentos/rnt/Finance/Trading Projects/00.database/tickmill/forex/'

dict_custos = {'WIN@N':0.2*5, 'WDO@N':2.40/2,
                                     'AUDUSD': 3,
                                     'EURUSD': 3,
                                     'GBPUSD': 3,
                                     'NZDUSD': 3,
                                     'USDCHF': 3,
                                     'USDJPY': 3,
                                     'AUDCAD': 3,
                                     'AUDCHF': 3,
                                     'AUDJPY': 3,
                                     'AUDNZD': 3,
                                     'CADCHF': 3,
                                     'CADJPY': 3,
                                     'CHFJPY': 3,
                                     'EURAUD': 3,
                                     'EURCAD': 3,
                                     'EURCHF': 3,
                                     'EURGBP': 3,
                                     'EURHKD': 3,
                                     'EURJPY': 3,
                                     'EURMXN': 3,
                                     'EURNZD': 3,
                                     'EURTRY': 3,
                                     'GBPCAD': 3,
                                     'GBPCHF': 3,
                                     'GBPJPY': 3,
                                     'USDCNH': 3,
                                     'USDCZK': 3,
                                     'USDMXN': 3,
                                     'USDTRY': 3,
                                     'XAGUSD': 3,
                                     'XAUUSD': 3,
                                     'STOXX50': 3,
                                     'UK100': 3,
                                     'FRANCE40': 3,
                                     'VIX': 3,
                                     'ALUMINIUM': 3,
                                     'PLATINUM': 3,
                                     'LEAD': 3,
                                     'NICKEL': 3,
                                     'PALLADIUM': 3,
                                     'ZINC': 3,
                                     'COPPER': 3,
                                     'DXY': 3,
                                     'COCOA': 3,
                                     'SUGAR': 3,
                                     'WHEAT': 3,
                                     'US500': 3,
                                     'NAT.GAS': 3,
                                     'BRENT': 3,
                                     'EEM': 3,
                                     'EWZ': 3,
                                     'IWM': 3,
                                     'TLT': 3,
                                     'SPY': 3,
                                     'DIA': 3}

dict_valor_lot = {'WIN@N':0.2, 'WDO@N':10.0,
                                'USDCAD': 100000,
                                 'AUDUSD': 100000,
                                 'EURUSD': 100000,
                                 'GBPUSD': 100000,
                                 'NZDUSD': 100000,
                                 'USDCHF': 100000,
                                 'USDJPY': 100000,
                                 'AUDCAD': 100000,
                                 'AUDCHF': 100000,
                                 'AUDJPY': 100000,
                                 'AUDNZD': 100000,
                                 'CADCHF': 100000,
                                 'CADJPY': 100000,
                                 'CHFJPY': 100000,
                                 'EURAUD': 100000,
                                 'EURCAD': 100000,
                                 'EURCHF': 100000,
                                 'EURGBP': 100000,
                                 'EURHKD': 100000,
                                 'EURJPY': 100000,
                                 'EURMXN': 100000,
                                 'EURNZD': 100000,
                                 'EURTRY': 100000,
                                 'GBPCAD': 100000,
                                 'GBPCHF': 100000,
                                 'GBPJPY': 100000,
                                 'USDCNH': 100000,
                                 'USDCZK': 100000,
                                 'USDMXN': 100000,
                                 'USDTRY': 100000,
                                 'XAGUSD': 100000,
                                 'XAUUSD': 100000,
                                 'STOXX50': 100000,
                                 'UK100': 100000,
                                 'FRANCE40': 100000,
                                 'VIX': 100000,
                                 'ALUMINIUM': 100000,
                                 'PLATINUM': 100000,
                                 'LEAD': 100000,
                                 'NICKEL': 100000,
                                 'PALLADIUM': 100000,
                                 'ZINC': 100000,
                                 'COPPER': 100000,
                                 'DXY': 100000,
                                 'COCOA': 100000,
                                 'SUGAR': 100000,
                                 'WHEAT': 100000,
                                 'US500': 100000,
                                 'NAT.GAS': 100000,
                                 'BRENT': 100000,
                                 'EEM': 100000,
                                 'EWZ': 100000,
                                 'IWM': 100000,
                                 'TLT': 100000,
                                 'SPY': 100000,
                                 'DIA': 100000}

dict_path = {'WIN@N':path_b3, 'WDO@N':path_b3, 
                              'USDCAD': path_tickmill,
                             'AUDUSD': path_tickmill,
                             'EURUSD': path_tickmill,
                             'GBPUSD': path_tickmill,
                             'NZDUSD': path_tickmill,
                             'USDCHF': path_tickmill,
                             'USDJPY': path_tickmill,
                             'AUDCAD': path_tickmill,
                             'AUDCHF': path_tickmill,
                             'AUDJPY': path_tickmill,
                             'AUDNZD': path_tickmill,
                             'CADCHF': path_tickmill,
                             'CADJPY': path_tickmill,
                             'CHFJPY': path_tickmill,
                             'EURAUD': path_tickmill,
                             'EURCAD': path_tickmill,
                             'EURCHF': path_tickmill,
                             'EURGBP': path_tickmill,
                             'EURHKD': path_tickmill,
                             'EURJPY': path_tickmill,
                             'EURMXN': path_tickmill,
                             'EURNZD': path_tickmill,
                             'EURTRY': path_tickmill,
                             'GBPCAD': path_tickmill,
                             'GBPCHF': path_tickmill,
                             'GBPJPY': path_tickmill,
                             'USDCNH': path_tickmill,
                             'USDCZK': path_tickmill,
                             'USDMXN': path_tickmill,
                             'USDTRY': path_tickmill,
                             'XAGUSD': path_tickmill,
                             'XAUUSD': path_tickmill,
                             'STOXX50': path_tickmill,
                             'UK100': path_tickmill,
                             'FRANCE40': path_tickmill,
                             'VIX': path_tickmill,
                             'ALUMINIUM': path_tickmill,
                             'PLATINUM': path_tickmill,
                             'LEAD': path_tickmill,
                             'NICKEL': path_tickmill,
                             'PALLADIUM': path_tickmill,
                             'ZINC': path_tickmill,
                             'COPPER': path_tickmill,
                             'DXY': path_tickmill,
                             'COCOA': path_tickmill,
                             'SUGAR': path_tickmill,
                             'WHEAT': path_tickmill,
                             'US500': path_tickmill,
                             'NAT.GAS': path_tickmill,
                             'BRENT': path_tickmill,
                             'EEM': path_tickmill,
                             'EWZ': path_tickmill,
                             'IWM': path_tickmill,
                             'TLT': path_tickmill,
                             'SPY': path_tickmill,
                             'DIA': path_tickmill}

def load_strategy_json(json_path):
    """
    Carrega um arquivo JSON de estratégia
    
    Args:
        json_path (str): Caminho para o arquivo JSON
        
    Returns:
        dict: Dados da estratégia
    """
    with open(json_path, 'r') as f:
        return json.load(f)

def get_strategy_function(strategy_name):
    """
    Retorna a função de estratégia baseada no nome
    
    Args:
        strategy_name (str): Nome da estratégia
        
    Returns:
        callable: Função de estratégia
    """
    # Mapear nomes de estratégias para funções em entries.py
    strategy_map = {
        'pattern_rsi_trend': entries.pattern_rsi_trend,
        'pattern_rsi_anti_trend': entries.pattern_rsi_anti_trend,
        'bb_trend': entries.bb_trend,
        'bb_anti_trend': entries.bb_anti_trend,
        'gold_rsi_trend': entries.gold_rsi_trend,
        # Adicione outras estratégias conforme necessário
    }
    
    if strategy_name in strategy_map:
        return strategy_map[strategy_name]
    else:
        raise ValueError(f"Estratégia '{strategy_name}' não encontrada no mapeamento")

def execute_backtest_for_hour(symbol, timeframe, strategy_data, hour_params, data_ini, data_fim, output_dir):
    """
    Executa backtest para uma hora específica
    
    Args:
        symbol (str): Símbolo do ativo
        timeframe (str): Timeframe
        strategy_data (dict): Dados gerais da estratégia
        hour_params (dict): Parâmetros específicos da hora
        data_ini (str): Data inicial do backtest
        data_fim (str): Data final do backtest
        output_dir (str): Diretório de saída
        
    Returns:
        pd.DataFrame: Resultados do backtest
    """
    # Extrair tp e sl dos parâmetros
    tp = hour_params.get('tp', 0.15)
    sl = hour_params.get('sl', 0.15)
    
    # Configurar o backtester
    bt = Backtester(
        symbol=symbol,
        timeframe=timeframe,
        data_ini=data_ini,
        data_fim=data_fim,
        tp=tp,
        sl=sl,
        slippage=0,
        tc=dict_custos.get(symbol, 0.5),
        lote=strategy_data.get('lote', 0.01),
        valor_lote=dict_valor_lot.get(symbol, 100000),
        initial_cash=30000,
        path_base=dict_path.get(symbol, './data/'),
        daytrade=strategy_data.get('daytrade', False)
    )
    
    # Obter função de estratégia
    strategy_function = get_strategy_function(strategy_data['strategy'])
    
    # Preparar argumentos da estratégia (remover tp e sl pois já foram passados ao backtester)
    signal_args = {k: v for k, v in hour_params.items() if k not in ['tp', 'sl']}
    
    # Executar backtest
    results, _ = bt.run(
        signal_function=strategy_function,
        signal_args=signal_args
    )
    
    return results

def process_combined_strategy(json_path, data_ini, data_fim, output_dir):
    """
    Processa um arquivo JSON de estratégia combinada
    
    Args:
        json_path (str): Caminho para o arquivo JSON
        data_ini (str): Data inicial do backtest
        data_fim (str): Data final do backtest
        output_dir (str): Diretório de saída
    """
    print(f"\nProcessando estratégia: {json_path}")
    
    # Carregar dados da estratégia
    strategy_data = load_strategy_json(json_path)
    
    # Extrair informações básicas
    if 'WIN' in strategy_data['symbol']:
        symbol = 'WIN@N'
    else:
        symbol = strategy_data['symbol']
    timeframe = strategy_data['timeframe']
    hours = strategy_data['hours']
    hour_params = strategy_data['hour_params']
    magic_number = strategy_data.get('magic_number', 'NO_MAGIC')  # Pegar magic_number ou usar padrão
    
    print(f"Símbolo: {symbol}")
    print(f"Timeframe: {timeframe}")
    print(f"Horas ativas: {hours}")
    print(f"Estratégia: {strategy_data['strategy']}")
    print(f"Magic Number: {magic_number}")
    
    # Dicionário para armazenar resultados de cada hora
    all_results = {}
    
    # Executar backtest para cada hora
    for hour in hours:
        str_hour = str(hour)
        if str_hour in hour_params:
            print(f"\nExecutando backtest para hora {hour:02d}...")
            
            params = hour_params[str_hour]
            params['allowed_hours'] = [hour]  # Adicionar hora permitida
            
            try:
                results = execute_backtest_for_hour(
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy_data=strategy_data,
                    hour_params=params,
                    data_ini=data_ini,
                    data_fim=data_fim,
                    output_dir=output_dir
                )
                
                # Armazenar resultados
                all_results[hour] = results
                
                print(f"Backtest da hora {hour:02d} concluído com sucesso")
                
            except Exception as e:
                print(f"Erro ao processar hora {hour}: {str(e)}")
                continue
    
    # Combinar resultados de todas as horas
    if all_results:
        print("\nCombinando resultados de todas as horas...")
        
        # Usar o primeiro resultado como base para a estrutura
        first_hour = list(all_results.keys())[0]
        combined_df = all_results[first_hour][['open', 'high', 'low', 'close']].copy()
        
        # Inicializar colunas combinadas
        combined_df['position'] = 0
        combined_df['strategy'] = 0.0
        combined_df['status_trade'] = 0
        combined_df['pts_final'] = 0.0
        
        # Combinar dados de cada hora
        for hour, hour_df in all_results.items():
            # Filtrar apenas as linhas da hora específica com posições
            hour_mask = (hour_df.index.hour == hour) & (hour_df['position'] != 0)
            
            # Atualizar dados combinados
            for idx in hour_df[hour_mask].index:
                if idx in combined_df.index:
                    combined_df.loc[idx, 'position'] = hour_df.loc[idx, 'position']
                    combined_df.loc[idx, 'strategy'] = hour_df.loc[idx, 'strategy']
                    combined_df.loc[idx, 'status_trade'] = hour_df.loc[idx, 'status_trade']
                    combined_df.loc[idx, 'pts_final'] = hour_df.loc[idx, 'pts_final']
        
        # Recalcular métricas acumuladas
        combined_df['cstrategy'] = combined_df['strategy'].cumsum()
        combined_df['equity'] = 30000 + combined_df['cstrategy']  # Initial cash = 30000
        
        # Salvar CSV combinado com magic_number no nome
        combined_csv_filename = f"backtest_{symbol}_{timeframe}_{strategy_data['strategy']}_magic_{magic_number}.csv"
        combined_csv_path = os.path.join(output_dir, combined_csv_filename)
        combined_df.to_csv(combined_csv_path)
        
        print(f"\nCSV combinado salvo em: {combined_csv_path}")
        print(f"Total de trades: {(combined_df['position'] != 0).sum()}")
        print(f"Resultado final: ${combined_df['cstrategy'].iloc[-1]:.2f}")
    
    else:
        print("Nenhum resultado foi gerado com sucesso.")

def main():
    """
    Função principal que processa todos os arquivos JSON de estratégias
    """
    # Configurações
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # Diretório do script atual
    OUTPUT_DIR = os.path.join(SCRIPT_DIR, "backtest_results")  # Pasta backtest_results no mesmo diretório
    DATA_INI = "2025-06-23"  # Data inicial do backtest
    DATA_FIM = "2025-12-31"  # Data final do backtest
    
    # Criar diretório de saída se não existir
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Iniciando processamento de estratégias...")
    print(f"Diretório do script: {SCRIPT_DIR}")
    print(f"Diretório de saída: {OUTPUT_DIR}")
    print(f"Período de backtest: {DATA_INI} a {DATA_FIM}")
    
    # Buscar arquivos .json apenas no diretório do script (não recursivo)
    json_files = glob.glob(os.path.join(SCRIPT_DIR, "*.json"))
    
    # Filtrar apenas arquivos combined_strategy.json ou outros arquivos de estratégia relevantes
    strategy_files = []
    for json_file in json_files:
        # Aceitar combined_strategy.json ou qualquer arquivo que contenha hour_params
        if 'combined_strategy' in os.path.basename(json_file):
            strategy_files.append(json_file)
        else:
            # Verificar se o arquivo contém a estrutura esperada
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    if 'hour_params' in data and 'hours' in data:
                        strategy_files.append(json_file)
            except:
                continue
    
    if not strategy_files:
        print(f"\nNenhum arquivo de estratégia válido encontrado em {SCRIPT_DIR}")
        print("Arquivos JSON encontrados:")
        for json_file in json_files:
            print(f"  - {os.path.basename(json_file)}")
        return
    
    print(f"\nEncontrados {len(strategy_files)} arquivos de estratégia válidos:")
    for file in strategy_files:
        print(f"  - {os.path.basename(file)}")
    
    # Processar cada arquivo JSON
    for json_file in strategy_files:
        try:
            print(f"\n{'='*60}")
            print(f"Processando: {os.path.basename(json_file)}")
            
            # Processar estratégia - todos os CSVs vão para OUTPUT_DIR
            process_combined_strategy(
                json_path=json_file,
                data_ini=DATA_INI,
                data_fim=DATA_FIM,
                output_dir=OUTPUT_DIR
            )
            
        except Exception as e:
            print(f"\nErro ao processar {os.path.basename(json_file)}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n{'='*60}")
    print("Processamento concluído!")
    print(f"Todos os CSVs foram salvos em: {OUTPUT_DIR}")
    
    # Listar arquivos gerados
    if os.path.exists(OUTPUT_DIR):
        csv_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.csv')]
        if csv_files:
            print(f"\nArquivos CSV gerados ({len(csv_files)}):")
            for csv_file in sorted(csv_files):
                print(f"  - {csv_file}")
        else:
            print("\nNenhum arquivo CSV foi gerado.")
    else:
        print(f"\nDiretório {OUTPUT_DIR} não foi criado.")

if __name__ == "__main__":
    main()