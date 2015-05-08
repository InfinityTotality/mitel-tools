class AgentGroup(object):
    def __init__(self):
        self.is_logged_in = None
        self.last_login = None

    def login(time):
        if self.is_logged_in == True:
            print('Warning: this state change was invalid')
        self.is_logged_in = True
        self.last_login = time


    def logout(time):
        if self.is_logged_in == False:
            print('Warning: this state change was invalid')
        self.is_logged_in = False
        self.last_logout = time


class ACDAgent(object):
    def __init__(self):
        self.extension = None
        self.is_logged_in = None
        self.last_login = None
        self.last_logout = None
        self.agent_groups = {}


    def login(extension, time):
        self.extension = extension
        if self.is_logged_in == True:
            print('Warning: this state change was invalid')
        self.is_logged_in = True
        self.last_login = time


    def logout(time):
        if self.is_logged_in == False:
            print('Warning: this state change was invalid')
        self.is_logged_in = False
        self.last_logout = time


    def login_to_group(group, time):
        if self.is_logged_in == False:
            print('Warning: logged in to agent group while not logged in')
        if group not in self.agent_groups:
            self.agent_groups[group] = AgentGroup()
        self.agent_groups[group].login(time)


    def logout_from_group(group, time):
        if self.is_logged_in == False:
            print('Warning: logged out of agent group while not logged in')
        if group not in self.agent_groups:
            self.agent_groups[group] = AgentGroup()
        self.agent_groups[group].logout(time)
