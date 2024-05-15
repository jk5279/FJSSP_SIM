import copy
import sys
import numpy as np


class Job:
    def __init__(self, id, operation_id, part_family, processing_time, machine_key_lst):
        self.job_id = id
        self.operation_id = operation_id
        self.part_family = part_family
        self.processing_time_lst = processing_time.tolist()
        self.processing_time = processing_time[np.where(processing_time != 0)][0]
        self.machine_key_lst = np.array(machine_key_lst)
        self.eligible_machine_id = self.__get_eligible_machine_id().tolist()
    def __get_eligible_machine_id(self):
        idx = np.where(self.processing_time_lst != 0)
        elgible_machine_id_lst = self.machine_key_lst[idx]
        return elgible_machine_id_lst
    def __str__(self):
        return f"Job {self.job_id} Operation {self.operation_id} Part Family {self.part_family} Processing Time {self.processing_time_lst} Eligible Machines {self.eligible_machine_id}"

class Machine:
    def __init__(self, cell_id, machine_id, machine_type):
        self.machine_cell_id = cell_id
        self.machine_id = machine_id
        self.machine_type = machine_type
        self.last_job_operation = None
        self.last_job_type = None

        self.machine_completion_time = 0

    def assign_job(self, job_id, job_operation_id, processing_time, setup_time, current_time):
        self.last_job_operation = (job_id, job_operation_id)
        self.machine_completion_time += processing_time + setup_time

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
        self.setup_df = self.__rename_setup_df_columns(self.__get_preproc_df(df_list[2]))
        self.transport_df = self.__get_preproc_df(df_list[3])
        self.duedate_df = self.__get_preproc_df(df_list[4])
        self.machine_key_lst = self.__get_machine_key_lst()
        self.job_operation_lst = []
        self.__get_job_operation_lst()

    def __rename_setup_df_columns(self, df):
        df.columns = ["From", "To", "Setup Time"]
        df["From"] = df["From"].astype(int) - 1
        df["To"] = df["To"].astype(int) - 1
        return df
    def __get_machine_key_lst(self):
        machine_cell_idx = self.job_df.columns[3:].tolist()
        machine_alternative_tuple_lst = []
        m_idx = 0
        for idx, m_alt_idx in enumerate(machine_cell_idx):
            if m_alt_idx == 1:
                m_idx += 1
            machine_alternative_tuple_lst.append((m_idx-1, m_alt_idx-1, idx))
        return machine_alternative_tuple_lst
    def __get_job_operation_lst(self):
        for row in self.job_df.values:
            part_family = row[0]
            job_id, operation_id = row[1], row[2]
            processing_time = row[3:]
            self.job_operation_lst.append(Job(job_id-1, operation_id-1, part_family-1, processing_time, self.machine_key_lst))

    def __get_preproc_df(self, df):
        _df = df.dropna(axis=1).rename(columns=df.iloc[0]).iloc[1:].reset_index(drop=True)
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
        self.job_operation_queue = copy.deepcopy(self.data.job_operation_lst)
        self.completed_job_operation_queue = []

    def allocate_job(self, job_id, job_operation_id, machine_alt_id):
        job_object = None
        for job in self.job_operation_queue:
            if job.job_id == job_id and job.operation_id == job_operation_id:
                job_object = job
                break
        processing_time = job_object.processing_time
        machine_cell_id = job_object.part_family
        machine_idx = machine_alt_id[2] % len(self.machine_cell_list[machine_cell_id])
        setup_time = 0
        if self.machine_cell_list[machine_cell_id][machine_idx].last_job_type == None:
            setup_time = 0
        else:
            setup_time = self.data.setup_df.loc[(self.data.setup_df["From"]==self.machine_cell_list[machine_cell_id][machine_idx].last_job_type) \
                                                & (self.data.setup_df["To"]==job_object.part_family),"Setup Time"][0]

        self.machine_cell_list[machine_cell_id][machine_idx].assign_job(job_id, job_operation_id, processing_time, setup_time, self.current_time)
        self.completed_job_operation_queue.append((job_id, job_operation_id))
        self.job_operation_queue.remove(job_object)
        self.__update_current_time()

    def __update_current_time(self):
        for cell in self.machine_cell_list:
            for machine in cell:
                if machine.machine_completion_time < self.current_time:
                    self.current_time = machine.machine_completion_time

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

    def get_available_machine_given_job_operation(self, job_id, job_operation_id):
        job_object = None
        for job in self.data.job_operation_lst:
            if job.job_id == job_id and job.operation_id == job_operation_id:
                job_object = job
                break
        return job_object.eligible_machine_id
    def get_available_machines(self):
        available_machines = []
        for cell in self.machine_cell_list:
            for machine in cell:
                if machine.is_available(self.current_time):
                    available_machines.append([machine.machine_cell_id, machine.machine_id, machine.machine_type])
        return available_machines
    def __reset_job_queue(self):
        self.job_operation_queue = copy.deepcopy(self.data.job_operation_lst)
        self.completed_job_operation_queue = []
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

