import json
import socket


class CommandNotFoundException(Exception):
    pass


class BaseSimulatedScript:
    socket = None

    def get_command_data_mapping(self):
        raise NotImplementedError

    def start(self):
        # How to start a using this function
        pass

    def stop(self):
        # How to stop and make sure it's not running
        pass

    def run_command(self, command):
        available_command_data_mapping = self.get_command_data_mapping()
        if command not in available_command_data_mapping.keys():
            raise CommandNotFoundException(f"Unable to find command {command} for {self.__class__.__name__}")
        #############This syntax is actually execute the command####################
        self.socket.sendall(available_command_data_mapping.get(command).encode('utf-8'))
        data_obj = self.socket.recv(2048).decode('utf-8')

    def setting_for_device(self):
        raise NotImplementedError

    def create_connection(self, ip_addr, port):
        try:
            print("Welcome")
            self.socket = socket.socket()
            print("Connecting.....")
            self.socket.connect((ip_addr, port))
            print('connected')
        except Exception as OSError:
            raise Exception(
                f'Oops! Not connected Some thing went wrong {OSError} please try again with valid IpAddress {ip_addr} and port {port}')


class SignalSharkSimulatedScript(BaseSimulatedScript):
    def setting_for_device(self):
        self.socket.sendall('*RST\r\n'.encode('utf-8'))
        print("Reset")
        self.socket.sendall('TASK:ADD? \'AUTO_DF\';VIEW:ADD? 1,RIGHT,\'MAP\'\r\n'.encode('utf-8'))
        print(self.socket.recv(2048).decode())
        self.socket.sendall('TASK:ADD? \'AUTO_DF\';VIEW:ADD? 2,RIGHT,\'PEAK_TABLE\'\r\n'.encode('utf-8'))
        print(self.socket.recv(2048).decode())
        print("Auto DF Added")
        self.socket.sendall(
            'TASK:SEL \'AUTO_DF\';SPEC:FREQ:CENT 2410 MHz;SPEC:FREQ:SPAN 40 MHz;SPEC:MEAS:TIME 10 ms\r\n'.encode(
                'utf-8'))
        print("Settings Set")
        open("../bearings.txt", "w+")
        open("../peaks.txt", "w+")

    def get_command_data_mapping(self):
        return {
            'BEARING-DATA': json.load('bearing-data.json'),
            'PEAKS-DATA': json.load('peaks-data.json'),
            'BEARING-PEAKS-DATA': json.load('bearing-peak-data.json')
        }


############################################MAIN FUNCTION######################3
if __name__ == "__main__":
    signal_shark_obj = SignalSharkSimulatedScript()
    bearing_command = "BEAR:DATA:ALL?\r\n"
    signal_shark_obj.create_connection('192.168.15.130', 5300)
    signal_shark_obj.setting_for_device()
    signal_shark_obj.run_command(bearing_command)
