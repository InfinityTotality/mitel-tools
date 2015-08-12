import acdreader
import acdagent
import operator
import sys
import re
from datetime import datetime

if len(sys.argv) < 4:
    print('Usage: ' + sys.argv[0] +
          ' start_date target_datetime acd_file_path [agent_id [...]]')
    exit()

if len(sys.argv) > 4:
    agent_filter = sys.argv[4:]
else:
    agent_filter = None

date_match = re.match('([0-9]{4}-[0-9]{2}-[0-9]{2}) ' +
                      '[0-9]{2}:[0-9]{2}:[0-9]{2}', sys.argv[2])
if date_match:
    end_date = date_match.group(1)
else:
    print('Invalid target datetime. Correct format is "YYYY-MM-DD HH:MM:SS"')
    exit()

try:
    reader = acdreader.ACDReader(sys.argv[3], sys.argv[1], end_date)
except acdreader.InvalidInputException as e:
    print('Error processing input: ' + str(e))
    exit()

target_time = datetime.strptime(sys.argv[2], '%Y-%m-%d %H:%M:%S')

agents = {}

def print_results():
    logged_in_agents = []
    logged_out_agents = []
    for agent in agents.values():
        if agent.is_logged_in == True:
            logged_in_agents.append(agent)
        elif agent.is_logged_in == False:
            logged_out_agents.append(agent)
    logged_in_agents.sort(key=operator.attrgetter('reporting'))
    logged_out_agents.sort(key=operator.attrgetter('reporting'))

    print('Logged in agents at {}:'
          .format(target_time.strftime('%Y-%m-%d %H:%M:%S')))
    for agent in logged_in_agents:
        print('Agent: {}\tExtension: {}\tLast login: {}'
              .format(agent.reporting, agent.extension,
              agent.last_login.strftime('%Y-%m-%d %H:%M:%S')))
    print('Known logged out agents:')
    for agent in logged_out_agents:
        print('Agent: {}\tExtension: {}\tLast logout: {}'
              .format(agent.reporting, agent.extension,
              agent.last_logout.strftime('%Y-%m-%d %H:%M:%S')))


def process_agent_login(line, current_time):
    agent = line[16:22].strip()
    if agent_filter is not None and agent not in agent_filter:
        #print('Agent not in filter list')
        return
    extension = line[9:16].strip()
    print('Agent ' + agent + ' logged in to extension ' + extension
          + ' at ' + current_time.strftime('%H:%M:%S'))
    if agent not in agents:
        agents[agent] = acdagent.ACDAgent(agent)
    agents[agent].login(extension, current_time)


def process_agent_logout(line, current_time):
    agent = line[16:22].strip()
    if agent_filter is not None and agent not in agent_filter:
        return
    extension = line[9:16].strip()
    print('Agent ' + agent + ' logged out at '
          + current_time.strftime('%H:%M:%S'))
    if agent not in agents:
        agents[agent] = acdagent.ACDAgent(agent)
    agents[agent].logout(current_time)


def process_group_event(line, current_time):
    line_parts = line.split('|')

    if (len(line_parts) < 12 or line_parts[6] != '3033' or
        line_parts[7] not in ('1007', '1008', '1011')):
        return
    agent = line_parts[4].replace('-', '').strip()
    if agent_filter is not None and agent not in agent_filter:
        return

    if agent not in agents:
            agents[agent] = acdagent.ACDAgent(agent)

    group = line_parts[10].strip()

    if line_parts[7] == '1007':
        print('Agent ' + agent + ' logged in to group ' + group +
              ' at ' + current_time.strftime('%H:%M:%S'))
        agents[agent].login_to_group(group, current_time)
    elif line_parts[7] == '1008':
        print('Agent ' + agent + ' logged out of group ' + group +
              ' at ' + current_time.strftime('%H:%M:%S'))
        agents[agent].logout_from_group(group, current_time)
    elif line_parts[7] == '1011':
        print('Agent ' + agent + ' logged out of all groups '
              ' at ' + current_time.strftime('%H:%M:%S'))
        agents[agent].logout_from_all_groups(current_time)


def process_files():
    for file in reader.file_reader():
        for bline in file:
            line = bline.decode('utf-8-sig')
            if line[2] in ('A', 'B', 'y'):
                current_time = datetime.strptime(
                        reader.current_date.strftime('%Y%m%d')
                        + line[3:9], '%Y%m%d%H%M%S')
                if current_time >= target_time:
                    return
            else:
                continue
            if line[2] == 'A':
                process_agent_login(line, current_time)
            elif line[2] == 'B':
                process_agent_logout(line, current_time)
            elif line[2] == 'y':
                process_group_event(line, current_time)


process_files()
print_results()
