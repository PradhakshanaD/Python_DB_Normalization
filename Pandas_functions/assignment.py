
# - ListV2
#  - __init__
#     - self.values
#  - __add__
#  - __sub__
#  - __mul__
#  - __truediv__
#  - append
#  - mean
#  - __iter__
#  - __next__
#  - __repr___
 
# - DataFrame
#  - __init__
#      - self.index - a dictionary to map text to row index
#      - self.data (dict of ListV2 where each column is a key)
#      - self.columns a simple list
#  - set_index
#  - __setitem__
#  - __getitem__
#  - loc
#  - iteritems
#  - iterrows
#  - as_type
#  - drop
#  - mean
#  - __repr__
import operator
class ListV2: 
    def __init__(self, values):
        if not isinstance(values, (list, tuple)):
            raise ValueError
        self.values = values

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index < len(self.values):
            result = self.values[self.index]
            self.index += 1
            return result
        else:
            raise StopIteration

    def arithmetic_operations(self, right, op):
        if isinstance(right, (int, float, list, tuple)):
            if isinstance(right, (int, float)):
                right = [right] * len(self.values)
            elif len(self.values) != len(right):
                raise ValueError
            return ListV2([op(l, r) for l, r in zip(self.values, right)])
        elif isinstance(right, ListV2):
            if len(self.values) == len(right.values):
                return ListV2([op(l, r) for l, r in zip(self.values, right.values)])
        else:
            raise ValueError
        
    def __len__(self):
        return len(self.values)     
        
    def sum(self):
        return sum(self.list)

    def count(self, value):
        return self.list.count(value)
    
    def __getitem__(self, key):
        return self.values[key]

    def __setitem__(self, key, value):
        self.values[key] = value
        

    def __add__(self, right):
        return self.arithmetic_operations(right, operator.add)

    def __sub__(self, right):
        return self.arithmetic_operations(right, operator.sub)

    def __mul__(self, right):
        return self.arithmetic_operations(right,  operator.mul)

    def __truediv__(self, right):
        return self.arithmetic_operations(right, lambda x, y: round(x / y, 1))
    
    def mean(self):
        return sum(self.values) / len(self.values)
    
    def append(self, value):
        self.values.append(value)
        self.mean = lambda: round(sum(self.values) / len(self.values))
        return self.mean()
    
    def __repr__(self):
        return repr(self.values)
    
class DataFrame:
    def __init__(self, data, columns,change_in_index=None,index=None):
        self.data = {}
        if columns is not None:
            self.columns = columns
            for i, col in enumerate(columns):
                self.data[col] = ListV2([row[i] for row in data])
        if index is not None:
            self.index = index
        else:
            self.index={item[0]: i for i, item in enumerate(data)} 
        if change_in_index is None:
            self.change_in_index = None
        else: 
            self.change_in_index= change_in_index

    def set_index(self, col):
        for i, row in enumerate(col):
            self.index[row] = i
        self.change_in_index=True
        
        
    def __setitem__(self, key, value):
        self.data[key] = ListV2(value)

    def __len__(self):
        return len(self.data[self.columns[0]])    

    def __getitem__(self, keys):
        if type(keys) in [str]:
            return self.data[keys]
        elif type(keys) in [list]:
            data = []
            for i in range(len(self.index)):
                values = [self.data[key].values[i] for key in keys]
                row = list(dict(zip(keys, values)).values())
                data.append(row)
            return DataFrame(data=data, columns=keys)
        elif type(keys) in [slice]:
            rows = list(zip(*self.data.values()))
            data = rows[keys.start:keys.stop:keys.step]
            return DataFrame(data=data, columns=self.columns)
        elif type(keys) in [tuple]:
            data = []
            rows = range(*keys[0].indices(len(self.index)))
            cols = range(*keys[1].indices(len(self.columns) - 1))
            for i in rows:
                row = [self.data[self.columns[j]].values[i] for j in cols]
                data.append(row)
            columns = [self.columns[j] for j in cols]
            return DataFrame(data=data, columns=columns)
      
    def loc(self, key):
        
        if isinstance(key, int):
            row_index = key
            return {col: self.data[col][row_index] for col in self.columns}
        elif isinstance(key, tuple):
            idx_rows, idx_cols = key
            idx_cols_idx = [self.columns.index(col) for col in idx_cols]
            new_data = []
            for row_key, row_idx in self.index.items():
                if row_key in idx_rows:
                    new_row = []
                    for idx in idx_cols_idx:
                        new_row.append(self.data[self.columns[idx]][row_idx])
                    new_data.append(new_row)
            latest_index={}
            for ele in idx_rows:
                latest_index[ele]=self.index[ele]
            return DataFrame(new_data, idx_cols,change_in_index=True,index=latest_index)

    def iteritems(self):
        output = {}
        for i in range(len(self.index)):
            row_data = tuple(self.data[col][i] for col in self.columns)
            for j, col in enumerate(self.columns):
                if col not in output:
                    output[col] = []
                output[col].append(row_data[j])
        return output

    def iterrows(self):
        rows = []
        for i in range(len(self.index)):
            row_data = tuple(self.data[col][i] for col in self.columns)
            row_index=list(self.index.keys())[i]
            rows.append((row_index, row_data))
        return rows

    def as_type(self, col, dtype):
        new_col_values = []
        for value in self.data[col]:
            new_col_values.append(dtype(value))
        self.data[col] = ListV2(new_col_values)

    def drop(self, col):
        del self.data[col]
        self.columns=[ele for ele in self.columns if ele!=col]

    def mean(self):
        return {col: round(sum(self.data[col]) / len(self),1) for col in self.data}
    
    def __repr__(self):
        s = ','.join(self.columns)
        rows = []
        if self.change_in_index is not None:
            for i in range(len(self.data[self.columns[0]])):
                row =[key for key in self.index.keys() if self.index[key]==i]
                for col in self.columns:
                    row.append(str(self.data[col].values[i]))
                rows.append(','.join(row))
        else:
            for i in range(len(self.data[self.columns[0]])):
                row = [str(i)]
                for col in self.columns:
                    row.append(str(self.data[col].values[i]))
                rows.append(','.join(row))
        return ',{}\n{}'.format(s, '\n'.join(rows))
