# tests/test_data_quality.py
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from scripts.utils import (
    log_quality_metric, calculate_basic_metrics, validate_data_quality_table
)

class TestDataQuality:
    
    @patch('scripts.utils.save_to_postgres')
    @patch('scripts.utils.get_last_update')
    def test_log_quality_metric_success(self, mock_last_update, mock_save):
        """Testa log de métrica de qualidade com sucesso"""
        mock_last_update.return_value = '2024-09-18'
        mock_save.return_value = None
        
        log_quality_metric('test_table', 'silver', 'null_percentage', 2.5, 'ok')
        
        assert mock_save.called
        call_args = mock_save.call_args[0]
        df_passed = call_args[0]
        
        assert df_passed['table_name'].iloc[0] == 'test_table'
        assert df_passed['metric_value'].iloc[0] == 2.5
        
    @patch('scripts.utils.save_to_postgres')
    @patch('scripts.utils.get_last_update')
    def test_log_quality_metric_failure(self, mock_last_update, mock_save):
        """Testa log de métrica de qualidade com erro"""
        mock_last_update.return_value = '2024-09-18'
        mock_save.side_effect = Exception('DB Error')
        
        with pytest.raises(Exception, match='DB Error'):
            log_quality_metric('test_table', 'silver', 'null_percentage', 2.5, 'ok')
    
    def test_calculate_basic_metrics(self, sample_df):
        """Testa cálculo de métricas básicas"""
        with patch('scripts.utils.log_quality_metric') as mock_log:
            calculate_basic_metrics(sample_df, 'companies', 'silver')
            
            # Verifica se as métricas foram logadas
            assert mock_log.call_count >= 3  # total_records + null_percentage + colunas
    
    def test_calculate_basic_metrics_empty_df(self):
        """Testa cálculo de métricas com DataFrame vazio"""
        empty_df = pd.DataFrame()
        
        with patch('scripts.utils.log_quality_metric') as mock_log:
            calculate_basic_metrics(empty_df, 'companies', 'silver')
            
            # Deve logar métricas mesmo com DataFrame vazio
            assert mock_log.called
    
    def test_validate_data_quality_table_success(self, sample_df):
        """Testa validação de qualidade com sucesso"""
        schema = DataFrameSchema({
            "cnpj": Column(str, nullable=False),
            "cod_porte": Column(str, checks=Check.isin(["00", "01", "03", "05"]))
        })
        
        with patch('scripts.utils.calculate_basic_metrics'), \
             patch('scripts.utils.log_quality_metric'):
            
            # Não deve levantar exceção
            validate_data_quality_table(sample_df, 'companies', 'silver', schema)
    
    def test_validate_data_quality_table_failure(self):
        """Testa validação de qualidade com falha"""
        invalid_df = pd.DataFrame({
            'cnpj': ['123', None],
            'cod_porte': ['99', '03']  # '99' não é válido
        })
        
        schema = DataFrameSchema({
            "cnpj": Column(str, nullable=False),
            "cod_porte": Column(str, checks=Check.isin(["00", "01", "03", "05"]))
        })
        
        with patch('scripts.utils.calculate_basic_metrics'), \
             patch('scripts.utils.log_quality_metric'):
            
            with pytest.raises(Exception, match='Data Quality falhou'):
                validate_data_quality_table(invalid_df, 'companies', 'silver', schema)
    
    def test_validate_data_quality_table_empty(self):
        """Testa validação com DataFrame vazio"""
        empty_df = pd.DataFrame()
        
        schema = DataFrameSchema({
            "cnpj": Column(str, nullable=False)
        })
        
        with patch('scripts.utils.calculate_basic_metrics'), \
             patch('scripts.utils.log_quality_metric'):
            
            # Deve falhar com DataFrame vazio que não atende schema
            with pytest.raises(Exception):
                validate_data_quality_table(empty_df, 'companies', 'silver', schema)