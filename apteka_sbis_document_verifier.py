import pandas as pd
import glob
import os
from datetime import datetime

# Пути к директориям
sbis_path = r'Входящие\Входящие'
apteki_path = r'Аптеки\Аптеки\csv\correct'
result_path = r'Результат'

# Получение текущей даты в формате ДД.ММ.ГГГГ
current_date = datetime.now().strftime('%d.%m.%Y')
result_folder = os.path.join(result_path, current_date)

# Создание папки для результатов, если её нет
os.makedirs(result_folder, exist_ok=True)

def load_all_sbis_files(folder_path):
    """Загружает и объединяет все CSV‑файлы СБИС из указанной папки."""
    print(f"\n=== ЗАГРУЗКА ФАЙЛОВ СБИС ===")
    file_paths = glob.glob(os.path.join(folder_path, "*.csv"))


    if not file_paths:
        print("✗ В папке не найдено CSV‑файлов СБИС!")
        return pd.DataFrame()

    print(f"Найдено файлов СБИС: {len(file_paths)}")

    dataframes = []
    successful_loads = 0

    for file_path in file_paths:
        try:
            df = pd.read_csv(
                file_path,
                sep=';',
                encoding='windows-1251',
                low_memory=False
            )
            dataframes.append(df)
            successful_loads += 1
            print(f"✓ Загружен файл СБИС: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"✗ Ошибка загрузки файла СБИС {file_path}: {e}")

    # Объединяем все DataFrame
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"\n✅ Успешно загружено файлов СБИС: {successful_loads}")
        print(f"Итоговый размер DataFrame СБИС: {combined_df.shape}")
        return combined_df
    else:
        print("\n✗ Не удалось загрузить ни одного файла СБИС!")
        return pd.DataFrame()

def load_all_apteka_files(folder_path):
    """Загружает и объединяет все CSV‑файлы аптек из указанной папки."""
    print(f"\n=== ЗАГРУЗКА ФАЙЛОВ АПТЕК ===")
    file_paths = glob.glob(os.path.join(folder_path, "*.csv"))

    if not file_paths:
        print("✗ В папке не найдено CSV‑файлов аптек!")
        return pd.DataFrame()
    print(f"Найдено файлов аптек: {len(file_paths)}")

    dataframes = []
    successful_loads = 0

    for file_path in file_paths:
        try:
            df = pd.read_csv(
                file_path,
                sep=';',
                encoding='windows-1251',
                low_memory=False
            )
            dataframes.append(df)
            successful_loads += 1
            print(f"✓ Загружен файл аптеки: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"✗ Ошибка загрузки файла аптеки {file_path}: {e}")

    # Объединяем все DataFrame
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"\n✅ Успешно загружено файлов аптек: {successful_loads}")
        print(f"Итоговый размер DataFrame аптек: {combined_df.shape}")
        return combined_df
    else:
        print("\n✗ Не удалось загрузить ни одного файла аптек!")
        return pd.DataFrame()

def process_combined_data(sbis_df, apteka_df):
    """Обрабатывает объединённые данные СБИС и аптек: сопоставляет документы по номеру накладной."""
    print(f"\n=== НАЧАЛО ОБРАБОТКИ ОБЪЕДИНЁННЫХ ДАННЫХ ===")


    if sbis_df.empty or apteka_df.empty:
        print("✗ Невозможно выполнить обработку — отсутствуют данные для сравнения!")
        return apteka_df


    # Добавляем новые столбцы для результатов
    apteka_df['Номер счет-фактуры'] = ''
    apteka_df['Сумма счет-фактуры'] = ''
    apteka_df['Дата счет-фактуры'] = ''
    apteka_df['Сравнение дат'] = ''

    processed_count = 0
    no_match_count = 0


    docs = ['СчФктр', 'УпдДоп', 'УпдСчфДоп', 'ЭДОНакл']


    for i, row in apteka_df.iterrows():
        # Модифицируем номер накладной для ЕАПТЕКА
        nakl = str(row['Номер накладной'])
        if 'ЕАПТЕКА' in str(row.get('Поставщик', '')):
            nakl += '/15'

        print(f"Обработка строки {i}: накладная {nakl}")

        # Ищем совпадения в СБИС
        records = sbis_df[sbis_df['Номер'] == nakl]
        records = records[records['Тип документа'].isin(docs)]


        if not records.empty:
            # Берём первый найденный документ
            invoice = records.iloc[0]['Номер']
            summ = records.iloc[0]['Сумма']

            # Обрабатываем дату из СБИС
            date_obj = pd.to_datetime(records.iloc[0]['Дата'])
            formatted_date = date_obj.strftime('%d.%m.%Y')
            apteka_df.at[i, 'Дата счет-фактуры'] = formatted_date

            print(f"✓ Найдено совпадение: счет-фактура {invoice}, дата {formatted_date}")

            # Сравниваем даты
            if pd.notna(row.get('Дата накладной')):
                doc_date = pd.to_datetime(row['Дата накладной'])
                if doc_date.date() != date_obj.date():
                    apteka_df.at[i, 'Сравнение дат'] = 'Не совпадает!'
                    print(f"⚠ Даты не совпадают: накладная {doc_date.strftime('%d.%m.%Y')} vs счет-фактура {formatted_date}")
                else:
                    apteka_df.at[i, 'Сравнение дат'] = 'Совпадает'
                    print(f"✓ Даты совпадают: {formatted_date}")
            else:
                apteka_df.at[i, 'Сравнение дат'] = 'Нет даты накладной'
                print("⚠ В данных аптеки нет даты накладной")

            # Заполняем остальные поля
            apteka_df.at[i, 'Номер счет-фактуры'] = invoice
            apteka_df.at[i, 'Сумма счет-фактуры'] = summ
            processed_count += 1
        else:
            # Если совпадений нет
            apteka_df.at[i, 'Сравнение дат'] = 'Не найдено в СБИС'
            no_match_count += 1
            print(f"✗ Совпадение не найдено для накладной {nakl}")

    print(f"\n✅ Обработка завершена:")
    print(f"Обработано записей с совпадениями: {processed_count}")
    print(f"Записей без совпадений в СБИС: {no_match_count}")

    return apteka_df


def main():
    """Основная функция скрипта."""
    # 1. Загружаем данные СБИС
    print("📥 Загрузка данных СБИС...")
    sbis_df = load_all_sbis_files(sbis_path)
    if sbis_df.empty:
        print("❌ Не удалось загрузить данные СБИС. Завершение работы.")
        return


    # 2. Загружаем данные аптек
    print("📥 Загрузка данных аптек...")
    apteka_df = load_all_apteka_files(apteki_path)
    if apteka_df.empty:
        print("❌ Не удалось загрузить данные аптек. Завершение работы.")
        return

    # 3. Выводим общую информацию о загруженных данных
    print("\n" + "="*50)
    print("ОБЩАЯ ИНФОРМАЦИЯ О ЗАГРУЖЕННЫХ ДАННЫХ")
    print("="*50)
    print(f"СБИС: {len(sbis_df)} строк, {len(sbis_df.columns)} столбцов")
    print(f"Аптеки: {len(apteka_df)} строк, {len(apteka_df.columns)} столбцов")

    # Промежуточная проверка — выводим заголовки DataFrames
    print("\n🔍 Заголовки DataFrame СБИС:")
    print(sbis_df.columns.tolist())
    print("\n🔍 Заголовки DataFrame Аптек:")
    print(apteka_df.columns.tolist())

    # 4. Обрабатываем данные (сопоставляем накладные с счёт‑фактурами)
    print("\n🚀 Начинаем обработку данных (сопоставление)...")
    processed_apteka_df = process_combined_data(sbis_df, apteka_df)
    if processed_apteka_df is None:
        print("❌ Обработка данных не выполнена.")
        return

    # 5. Сохраняем результат в CSV
    output_filename = f"обработанные_данные_аптек_{current_date}.csv"
    output_path = os.path.join(result_folder, output_filename)

    try:
        processed_apteka_df.to_csv(
            output_path,
            index=False,
            encoding='utf-8-sig',  # для корректного отображения кириллицы в Excel
            sep=';'
        )
        print(f"\n✅ Данные успешно сохранены: {output_path}")
        print(f"Размер файла: {processed_apteka_df.shape[0]} строк, {processed_apteka_df.shape[1]} столбцов")
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        return

    # 6. Финальная статистика
    print("\n" + "="*50)
    print("ФИНАЛЬНАЯ СТАТИСТИКА")
    print("="*50)

    # Статистика по заполненным номерам счёт‑фактур
    filled_invoices = (processed_apteka_df['Номер счет-фактуры'] != '').sum()
    total_records = len(processed_apteka_df)
    fill_rate = (filled_invoices / total_records * 100) if total_records > 0 else 0
    print(f"\n💳 Заполнение номеров счёт-фактур:")
    print(f"  Заполнено: {filled_invoices} из {total_records} записей")
    print(f"  Процент заполнения: {fill_rate:.1f}%")

    # Статистика по суммам
    try:
        # Преобразуем столбец сумм в числовой формат, игнорируя ошибки
        invoice_amounts = pd.to_numeric(
            processed_apteka_df['Сумма счет-фактуры'],
            errors='coerce'
        )
        total_invoice_amount = invoice_amounts.sum()
        average_invoice_amount = invoice_amounts.mean()
        print(f"\n💰 Общая сумма по счёт-фактурам: {total_invoice_amount:,.2f} руб.")
        print(f"  Средняя сумма по счёт-фактуре: {average_invoice_amount:,.2f} руб.")
    except Exception as e:
        print(f"\n⚠️ Не удалось рассчитать статистику по суммам: {e}")

    # Дополнительная статистика по поставщикам
    print(f"\n👥 Статистика по поставщикам:")
    supplier_stats = processed_apteka_df['Поставщик'].value_counts()
    for supplier, count in supplier_stats.items():
        print(f"  {supplier}: {count} записей")


    # Статистика по статусам сопоставления
    print(f"\n📊 Детализация по результатам сопоставления:")
    match_stats = processed_apteka_df['Сравнение дат'].value_counts(dropna=False)
    for status, count in match_stats.items():
        if pd.isna(status):
            print(f"  Без статуса: {count} записей")
        else:
            print(f"  {status}: {count} записей")

    print("\n🎉 Обработка завершена успешно!")
    print(f"Результаты сохранены в папке: {result_folder}")


# Точка входа скрипта
if __name__ == "__main__":
    print("🏘️ Старт скрипта обработки данных СБИС и аптек")
    print(f"Дата выполнения: {current_date}")
    print(f"Папка результатов: {result_folder}\n")
    main()

