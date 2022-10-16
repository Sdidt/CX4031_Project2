import pandas as pd

def read_tbl_and_dump_into_csv(filenames):
    for filename in filenames:
        df = pd.read_csv(f"./Database/{filename}.tbl", delimiter="|", header=None)
        df = df.iloc[:, :-1]
        df.to_csv(f"./Database/{filename}.csv", index=False, header=False)

# df = pd.read_csv("./Database/customer.tbl", delimiter="|", header=None)
# df = df.iloc[:, :-1]
# print(df.sample(5))
# print(df.describe())
# df.to_csv(f"./Database/customer.csv", index=False, header=False)
# new_df = pd.read_csv(f"./Database/customer.csv", header=None)
# print(new_df)

read_tbl_and_dump_into_csv(["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"])

