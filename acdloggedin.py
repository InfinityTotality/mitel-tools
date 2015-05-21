import acdreader
import acdagent
import operator
import sys
import re
from datetime import datetime

if len(sys.argv) < 4:
    print('Usage: ' + sys.argv[0] + ' start_date target_datetime acd_file_path [agent_id [...]]')
    exit()

if len(sys.argv) > 4:
    agent_filter = sys.argv[4:]
else:
    agent_filter = None

date_match = re.match('([0-9]{4}-[0-9]{2}-[0-9]{2}) [0-9]{2}:[0-9]{2}:[0-9]{2}', sys.argv[2])
if date_match:
    end_date = date_match.group(1)
else:
    print('Invalid target date')
    exit()

try:
    reader = acdreader.ACDReader(sys.argv[3], sys.argv[1], end_date)
except acdreader.InvalidInputException as e:
    print('Error processing input: ' + e.value)
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



def process_files():
    for file in reader.file_reader():
        for bline in file:
            line = bline.decode(sys.stdout.encoding)
            if line[2] == 'A' or line[2] == 'B':
                current_time = datetime.strptime(
                        reader.current_date.strftime('%Y%m%d')
                        + line[3:9], '%Y%m%d%H%M%S')
                if current_time >= target_time:
                    return
                agent = line[15:21].strip()
                if agent_filter is not None and agent not in agent_filter:
                    continue
                extension = line[9:15].strip()
                if line[2] == 'A':
                    print('Agent ' + agent + 
                          ' logged in to extension ' + extension +
                          ' at ' + current_time.strftime('%H:%M:%S'))
                    if agent not in agents:
                        agents[agent] = acdagent.ACDAgent(agent)
                    agents[agent].login(extension, current_time)
                elif line[2] == 'B':
                    print('Agent ' + agent + ' logged out at '
                          + current_time.strftime('%H:%M:%S'))
                    if agent not in agents:
                        agents[agent] = acdagent.ACDAgent(agent)
                    agents[agent].logout(current_time)
                else:
                    print('Invalid event: ' + line[2])
            elif line[2] == 'y':
                current_time = datetime.strptime(
                        reader.current_date.strftime('%Y%m%d')
                        + line[3:9], '%Y%m%d%H%M%S')
                if current_time >= target_time:
                    return
                line_parts = line.split('|')
                if len(line_parts) < 12 or line_parts[6] != '3033'
                or (line_parts[7] != '1007' and line_parts[7] != '1008'):
                    continue
                extension = line_parts[4].replace('-', '').strip()
                agent = line_parts[11].strip()
                if agent_filter is not None and agent not in agent_filter:
                    continue
                group = line_parts[10].strip()
                if line_parts[7] == 1007:
                    print('Agent ' + agent + 
                          ' logged in to group ' + group + ' at '
                          + current_time.strftime('%Y-%m-%d %H:%M:%S'))
                    if agent not in agents:
                        agents[agent] = acdagent.ACDAgent(agent)
                    agents[agent].login_to_group(group, current_time)
                elif line_parts[8] == 1008:
                    print('Agent ' + agent + 
                          ' logged out of group ' + group + ' at '
                          + current_time.strftime('%Y-%m-%d %H:%M:%S'))
                    if agent not in agents:
                        agents[agent] = acdagent.ACDAgent(agent)
                    agents[agent].logout_from_group(group, current_time)


process_files()
print_results()
