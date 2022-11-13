import pandas as pd
import re

def details_csv(df_name:str,df: pd.DataFrame):

    # Analisis calidad de los datos

    print('ANALISIS CALIDAD DE LOS DATOS\n')
    print(f'CSV {df_name}\n')

    print(df.info())
    print('\n')

    print(df.head(10))
    print('\n')

    print('Dimensiones del Csv\n')

    print(df.shape)
    print('\n')

    print('Columnas del Csv\n')

    print(df.columns)
    print('\n')

    print('Datos nulos del Csv\n')

    print(df.isnull())

    input()


def extract(csv_file: str) -> pd.DataFrame:  # Extraemos los datos de los csv

    df = pd.read_csv(csv_file)

    return df


def transform_orders(df: pd.DataFrame):  # Transformamos los datos de los dataframes

    pizza_rep = dict()
    df_pizza_quantity = df.loc[:, ['pizza_id', 'quantity']]  # Nos guardamos las columnas que nos interesan del dataframe
    list_pizza_quantity = df_pizza_quantity.values.tolist()  # Pasamos el dataframe a una lista

    # Eliminamos con expresiones regulares el tamaño de la pizza que acompaña al nombre de la pizza

    for i in range(len(list_pizza_quantity)):
        list_pizza_quantity[i][0] = re.sub('_[a-z]$', '', list_pizza_quantity[i][0])

    # Creamos un diccionario con claves los nombres de las pizzas y sus valores el número de veces que fueron pedidas.

    for i in range(len(list_pizza_quantity)):
        try:
            pizza_rep[list_pizza_quantity[i][0]] += 1
        except:
            pizza_rep[list_pizza_quantity[i][0]] = 1

    return pizza_rep


def transform_ingredients(df: pd.DataFrame, dict_orders:dict):

    dict_ingredients = dict()
    df_pizza_ingredients = df.loc[:, ['pizza_type_id','ingredients']]
    list_pizza_ingredients = df_pizza_ingredients.values.tolist()

    for i in range(len(list_pizza_ingredients)):
        list_pizza_ingredients[i][1] = list_pizza_ingredients[i][1].replace(', ', ',')
        list_pizza_ingredients[i][1] = re.findall(r'([^,]+)(?:,|$)', list_pizza_ingredients[i][1])
        # list_pizza_ingredients[i] = [nombre pizza,[ingrediente(1),...,ingrediente(k)]]

    for typepizza in range(len(list_pizza_ingredients)):

        for ingredients in range(len(list_pizza_ingredients[typepizza][1])):

            number_order_pizza = dict_orders[list_pizza_ingredients[typepizza][0]]  # Numero de veces que se pedio la pizza con dicho ingrerdiente

            try:
                dict_ingredients[list_pizza_ingredients[typepizza][1][ingredients]] = number_order_pizza + dict_ingredients[list_pizza_ingredients[typepizza][1][ingredients]]
            except:
                dict_ingredients[list_pizza_ingredients[typepizza][1][ingredients]] = number_order_pizza

    return dict_ingredients


if __name__ == '__main__':


    # Seguiremos el preceso ETL

    # Extract
    order_details = extract('order_details.csv')
    pizzas = extract('pizzas.csv')
    pizza_types = extract('pizza_types.csv')

    # Analisis
    details_csv('order_details.csv',order_details)
    details_csv('pizzas.csv',pizzas)
    details_csv('pizza_types.csv',pizza_types)

    # Transform
    dict_pizza_orders = transform_orders(order_details)
    dict_ingredients_anual = transform_ingredients(pizza_types, dict_pizza_orders)

    # Ingredients semanales

    dict_ingredients_weekly = dict()

    for key in dict_ingredients_anual:

        dict_ingredients_weekly[key] = dict_ingredients_anual[key] / 12 * 4

    print('Para stock de ingredientes deberian comprar a la semana -->\n')
    print('El consumo de cada ingredientes medio por semana es de: ')

    for key in dict_ingredients_anual:
        print(f'{key}: {int(dict_ingredients_weekly[key])}')
