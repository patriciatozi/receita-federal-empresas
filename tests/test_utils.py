# tests/test_utils.py
import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import io
from scripts.utils import (
    get_response, get_compiled_pattern, get_last_folder,
    get_last_update, get_source_data, save_to_postgres,
    read_table, check_for_duplicates, get_database_connection,
    save_df_to_parquet
)

class TestUtils:
    
    @patch('scripts.utils.requests.get')
    def test_get_response_success(self, mock_get):
        """Testa get_response com sucesso"""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        response = get_response('https://test.com')
        
        assert response == mock_response
        mock_get.assert_called_once_with('https://test.com')
        mock_response.raise_for_status.assert_called_once()
    
    @patch('scripts.utils.requests.get')
    def test_get_response_failure(self, mock_get):
        """Testa get_response com erro HTTP"""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception('HTTP Error')
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match='HTTP Error'):
            get_response('https://test.com')
    
    def test_get_compiled_pattern(self, mock_response):
        """Testa compilação de padrão regex"""
        pattern, soup = get_compiled_pattern(mock_response, r'^\d{4}-\d{2}/$')
        assert pattern.pattern == r'^\d{4}-\d{2}/$'
        assert soup is not None
    
    @patch('scripts.utils.get_response')
    def test_get_last_folder(self, mock_get_response):
        """Testa obtenção da última pasta"""
        mock_response = MagicMock()
        mock_response.text = '''
        <html>
            <a href="2024-09/">2024-09</a>
            <a href="2024-08/">2024-08</a>
        </html>
        '''
        mock_get_response.return_value = mock_response
        
        last_folder = get_last_folder('https://test.com')
        
        assert last_folder == '2024-09'
        mock_get_response.assert_called_once_with('https://test.com')
    
    def test_get_last_update(self):
        """Testa obtenção da data atual"""
        import datetime
        with patch('datetime.date') as mock_date:
            mock_date.today.return_value = datetime.date(2024, 9, 18)
            result = get_last_update()
            assert result == datetime.date(2024, 9, 18)
    
    @patch('scripts.utils.get_response')
    @patch('scripts.utils.get_last_folder')
    @patch('scripts.utils.zipfile.ZipFile')
    @patch('scripts.utils.pd.read_csv')
    def test_get_source_data(self, mock_read_csv, mock_zipfile, mock_get_last_folder, mock_get_response):
        """Testa obtenção de dados da fonte"""
        # Setup dos mocks
        mock_get_last_folder.return_value = '2024-09'
        
        mock_zip = MagicMock()
        mock_zip.namelist.return_value = ['Empresas.csv']
        mock_zipfile.return_value.__enter__.return_value = mock_zip
        
        mock_df = pd.DataFrame({
            'cnpj': ['12345678000195'],
            'razao_social': ['Test Company']
        })
        mock_read_csv.return_value = mock_df
        
        # Mock da resposta HTTP para get_last_folder
        mock_html_response = MagicMock()
        mock_html_response.text = '''
        <html>
            <a href="Empresas0.zip">Empresas0.zip</a>
            <a href="Empresas1.zip">Empresas1.zip</a>
        </html>
        '''
        mock_get_response.return_value = mock_html_response
        
        columns = ['cnpj', 'razao_social']
        dtypes = {'cnpj': str, 'razao_social': str}
        
        result = get_source_data('Empresas', columns, dtypes)
        
        assert len(result) == 1
        assert 'last_update' in result.columns
        assert result['last_update'].iloc[0] == '2024-09'
    
    def test_save_to_postgres(self, sample_df):
        """Testa salvamento no PostgreSQL"""
        with patch('scripts.utils.get_database_connection') as mock_conn, \
             patch('psycopg2.extras.execute_values') as mock_execute:
            
            mock_cursor = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cursor
            mock_conn.return_value.commit.return_value = None
            
            columns_table = {
                'cnpj': 'TEXT PRIMARY KEY',
                'razao_social': 'TEXT'
            }
            
            save_to_postgres(sample_df, 'test_table', columns_table, ['cnpj'], 'update')
            
            assert mock_cursor.execute.called
            assert mock_conn.return_value.commit.called
    
    def test_read_table(self):
        """Testa leitura de tabela do PostgreSQL"""
        with patch('scripts.utils.get_database_connection') as mock_conn, \
             patch('pandas.read_sql') as mock_read_sql:
            
            mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            mock_read_sql.return_value = mock_df
            
            result = read_table('test_table', ['col1', 'col2'], "col1 > 1")
            
            assert len(result) == 2
            mock_read_sql.assert_called_once()
    
    def test_check_for_duplicates_no_duplicates(self, sample_df):
        """Testa verificação de duplicatas sem duplicatas"""
        result = check_for_duplicates(sample_df)
        assert result is True
    
    def test_check_for_duplicates_with_duplicates(self):
        """Testa verificação de duplicatas com duplicatas"""
        df_with_duplicates = pd.DataFrame({
            'cnpj': ['123', '123', '456'],
            'nome': ['A', 'A', 'B']
        })
        
        result = check_for_duplicates(df_with_duplicates)
        assert result is False
    
    def test_get_database_connection(self):
        """Testa criação de conexão com banco"""
        with patch('psycopg2.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            conn = get_database_connection()
            
            assert conn == mock_conn
            mock_connect.assert_called_once_with(
                host='localhost',
                port=5432,
                dbname='test_db',
                user='test_user',
                password='test_pass'
            )
    
    def test_save_df_to_parquet(self, sample_df, tmp_path):
        """Testa salvamento de DataFrame em Parquet"""
        test_path = tmp_path / "test.parquet"
        
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            save_df_to_parquet(str(test_path), sample_df, ['cod_porte'])
            
            mock_to_parquet.assert_called_once_with(
                str(test_path), engine="pyarrow", index=False, partition_cols=['cod_porte']
            )

def test_get_source_data_test_mode():
    """Testa get_source_data em modo teste"""
    columns = ['cnpj', 'razao_social']
    dtypes = {'cnpj': str, 'razao_social': str}
    
    result = get_source_data('Empresas', columns, dtypes, test_mode=True)
    
    assert len(result) == 2
    assert 'cnpj' in result.columns
    assert 'razao_social' in result.columns