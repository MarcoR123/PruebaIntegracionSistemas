import pandas as pd
import pyodbc
import os
import shutil  # Importar shutil para manejar operaciones de archivo
import datetime  # Importar datetime para manejar fechas y horas

try:
    # Configuración de la conexión a la base de datos usando autenticación de Windows
    server = 'M-ALIENWARE'
    database = 'ElectroFacil'
    cnxn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

    # Carpeta donde están los archivos CSV
    folder_path = 'C:/PruebaIntegracionSistemas/Origen'
    # Carpeta de destino para mover archivos
    destination_folder = 'C:/PruebaIntegracionSistemas/Destino'

    # Leer todos los archivos CSV en la carpeta
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
    df_list = [pd.read_csv(file) for file in all_files]
    ventas_df = pd.concat(df_list, ignore_index=True)

    # Conectar a la base de datos
    cnxn = pyodbc.connect(cnxn_string)
    cursor = cnxn.cursor()

    try:
        # Limpiar la tabla antes de insertar nuevos datos
        cursor.execute("TRUNCATE TABLE Ventas_Consolidadas")
        cnxn.commit()

        # Insertar los datos en la base de datos
        for index, row in ventas_df.iterrows():
            cursor.execute("INSERT INTO Ventas_Consolidadas (IdLocal, IdTransaccion, Fecha, IdCategoria, IdProducto, Producto, Cantidad, PrecioUnitario, TotalVenta) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                           row['IdLocal'], row['IdTransaccion'], row['Fecha'], row['IdCategoria'], row['IdProducto'], row['Producto'], row['Cantidad'], row['PrecioUnitario'], row['TotalVenta'])
            cnxn.commit()
    except Exception as e:
        print("Error durante la inserción de datos:", e)
    finally:
        cursor.close()
        cnxn.close()

    print("Datos cargados exitosamente en la base de datos.")

    # Preguntar al usuario si desea mover los archivos
    respuesta = input("¿Deseas mover los archivos CSV a otra carpeta? (S/N): ")
    if respuesta.upper() == 'S':
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Obtener la fecha y hora actual
        for file in all_files:
            base_name = os.path.basename(file)
            new_name = f"{os.path.splitext(base_name)[0]}_{current_time}{os.path.splitext(base_name)[1]}"
            new_path = os.path.join(destination_folder, new_name)
            shutil.move(file, new_path)
        print(f"Archivos movidos a {destination_folder} con la fecha y hora agregadas.")
    else:
        print("No se movieron los archivos.")

except Exception as e:
    print("Error en el proceso general:", e)
