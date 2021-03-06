class AgentGroup(object):
    def __init__(self):
        self.is_logged_in = None
        self.last_login = None

    def login(self, time):
        if self.is_logged_in == True:
            print('Warning: this state change was invalid')
        self.is_logged_in = True
        self.last_login = time


    def logout(self, time):
        if self.is_logged_in == False:
            print('Warning: this state change was invalid')
        self.is_logged_in = False
        self.last_logout = time


class ACDAgent(object):
    def __init__(self, reporting):
        self.reporting = reporting
        self.extension = None
        self.is_logged_in = None
        self.last_login = None
        self.last_logout = None
        self.agent_groups = {}


    def login(self, extension, time):
        self.extension = extension
        if self.is_logged_in == True:
            print('Warning: this state change was invalid')
        self.is_logged_in = True
        self.last_login = time


    def logout(self, time):
        if self.is_logged_in == False:
            print('Warning: this state change was invalid')
        self.is_logged_in = False
        self.last_logout = time


    def login_to_group(self, group, time):
        if self.is_logged_in == False:
            print('Warning: logged in to agent group while not logged in')
        if group not in self.agent_groups:
            self.agent_groups[group] = AgentGroup()
        self.agent_groups[group].login(time)


    def logout_from_group(self, group, time):
        if self.is_logged_in == False:
            print('Warning: logged out of agent group while not logged in')
        if group not in self.agent_groups:
            self.agent_groups[group] = AgentGroup()
        self.agent_groups[group].logout(time)


    def logout_from_all_groups(self, time):
        if self.is_logged_in == False:
            print('Warning: logged out of agent group while not logged in')
        for group in self.agent_groups.values():
            group.logout(time)
