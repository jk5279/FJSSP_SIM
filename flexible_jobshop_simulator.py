import sys


class Job:
    def __init__(self, id, family, processing_time):
        self.id = id
        self.family = family
        self.processing_time = processing_time

    def __str__(self):
        return f"Job {self.id} ({self.family}): {self.processing_time}"


class Machine:
    def __init__(self, cell_id, machine_id, machine_type):
        self.machine_cell_id = cell_id
        self.machine_id = machine_id
        self.machine_type = machine_type
        self.last_job_operation = None
        self.last_job_type = None

        self.machine_completion_time = 0

    def assign_job(self, job_id, job_operation_id, processing_time, current_time):
        self.last_job_operation = (job_id, job_operation_id)
        self.machine_completion_time += processing_time

    def is_available(self, current_time):
        return self.machine_completion_time <= current_time
    def __str__(self):
        return f"Machine {self.machine_id}-(Type {self.machine_type}) in Cell {self.machine_cell_id} is available at {self.next_avail_time} for Job {self.job}"

def split_dataframe(df, indices):
    start = 0
    df_list = []
    for index in indices:
        df_list.append(df.iloc[start:index])
        start = index + 1
    df_list.append(df.iloc[start:])
    return df_list

class Data:
    def __init__(self, df_list):
        self.attribute_df = df_list[0]
        self.job_df = self.__get_job_df(df_list[1])
        self.job_index_lst = self.job_df.iloc[:, 1].unique().tolist()
        self.job_operation_index_lst = self.job_df.iloc[:,1:3].values.tolist()
        self.cell_machine_type_list = df_list[1].iloc[:2, :-1].dropna(axis=1).astype(int).values.tolist()
        self.setup_df = self.__get_preproc_df(df_list[2])
        self.transport_df = self.__get_preproc_df(df_list[3])
        self.duedate_df = self.__get_preproc_df(df_list[4])

    def __get_preproc_df(self, df):
        _df = df.dropna(axis=1).rename(columns=df.iloc[0]).iloc[1:]
        return _df
    def __get_job_df(self, df):
        header_lst = df.iloc[2, :-1].values.tolist()
        header_lst[3:] = [int(i) for i in header_lst[3:]]
        _df = df.iloc[3:, :-1].fillna(0).astype(int)
        _df.columns = header_lst
        return _df

class FJSS_SIMULATOR:
    def __init__(self, data, instance):
        nan_row_index = list(data[data.isna().all(axis=1)].index)
        df_list = split_dataframe(data, nan_row_index)
        self.data = Data(df_list)

        properties = ['N', 'M', 'L', 'C', 'opi']
        attributes = ['n', 'm', 'L', 'C', f'op{instance}']
        for attr, prop in zip(attributes, properties):
            setattr(self, prop, self.data.attribute_df.loc[self.data.attribute_df[0] == attr, 1].astype(int).iloc[0])

        self.machine_cell_list = self.__get_init_machine_states()
        self.current_time = 0
        self.job_operation_queue = self.data.job_operation_index_lst
        self.job_queue = self.data.job_index_lst
        self.completed_jobs = []

        self.get_available_machine_given_job_operation(2, 1)
        exit()
        self.allocate_job(22, 1, 1, 1)
    def allocate_job(self, job_id, job_operation_id, machine_cell_id, machine_id):
        processing_time = self.data.job_df.loc[
            (self.data.job_df["Job"] == job_id) and (self.data.job_df["Operation no"] == job_operation_id)]
        setup_time = 0 if self.machine_cell_list[machine_cell_id][machine_id].last_job_type == None else self.data.setup_df.loc[self.data.setup_df["From"] == self.machine_cell_list[machine_cell_id][machine_id].last_job_type].loc[self.data.setup_df["To"] == job_id]
        exit()
        self.machine_cell_list[machine_cell_id][machine_id].assign_job(job_id, job_operation_id, processing_time, self.current_time)
        self.current_time = self.machine_cell_list[machine_cell_id][machine_id].machine_completion_time
    def get_available_job_operations(self):
        available_job_operation = []
        for job_idx in self.job_queue:
            if job_idx in self.completed_jobs:
                continue
            for job_operation in self.job_operation_queue:
                if job_operation[0] == job_idx:
                    available_job_operation.append(job_operation)
                    break
        return available_job_operation

    # def get_available_machine_given_job_operation(self, job_id, job_operation_id):
    #     df = self.data.job_df.loc[(self.data.job_df["Job"] == job_id) & (self.data.job_df["Operation no"] == job_operation_id)].iloc[:, 3:].values
    #     available_machines = []
    #     for machine_cell_id, machine_id in df.iloc[:, :2].values:
    #         if self.machine_cell_list[machine_cell_id][machine_id].is_available(self.current_time):
    #             available_machines.append([machine_cell_id, machine_id])
    #     print(available_machines)
    #     return available_machines
    def get_available_machines(self):
        available_machines = []
        for cell in self.machine_cell_list:
            for machine in cell:
                if machine.is_available(self.current_time):
                    available_machines.append([machine.machine_cell_id, machine.machine_id, machine.machine_type])
        return available_machines
    def __reset_job_queue(self):
        self.job_operation_queue = self.data.job_operation_index_lst
        self.job_queue = self.data.job_index_lst
        self.completed_jobs = []
    def __reset_current_time(self):
        self.current_time = 0
    def __print_current_machine_states(self):
        for cell in self.machine_cell_list:
            for machine in cell:
                print(machine)

    def __get_init_machine_states(self):
        cell_lst = [[] for _ in range(self.C)]
        L_idx = 1
        for machine_id, cell_id in enumerate(self.data.cell_machine_type_list[0]):
            cell_lst[cell_id - 1].append(
                Machine(cell_id - 1, machine_id, self.data.cell_machine_type_list[1][machine_id] - 1))
            if cell_id != L_idx:
                L_idx += 1
        return cell_lst

# def __str__(self):
#      return f"N: {self.N}, M: {self.M}, L: {self.L}, C: {self.C}, opi: {self.opi}"
