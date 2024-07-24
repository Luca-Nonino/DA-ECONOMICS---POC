import os
from crewai import Agent, Task, Crew, Process

# Set up your OpenAI API key
os.environ["OPENAI_API_KEY"] = "sk-proj-zs7VeTanFjRpqor4lNbCT3BlbkFJUsJK7PzmHZp5UVMwX1Vm"
os.environ['OPENAI_MODEL_NAME'] = 'gpt-4o-mini'
# Define your agents with roles and goals
researcher = Agent(
  role='Scientific Researcher',
  goal='Gather and analyze information about the hollow Earth theory and scientific evidence related to Earth\'s structure',
  backstory="""You are a diligent scientific researcher with expertise in geology and Earth sciences. 
  Your task is to objectively examine the hollow Earth theory and compare it with established scientific knowledge.""",
  verbose=True,
  allow_delegation=False
)

analyst = Agent(
  role='Data Analyst',
  goal='Analyze the gathered information and draw conclusions based on scientific evidence',
  backstory="""You are a skilled data analyst with a background in Earth sciences. 
  Your role is to critically examine the information collected about the hollow Earth theory and current scientific understanding.""",
  verbose=True,
  allow_delegation=True
)

# Create tasks for your agents
task1 = Task(
  description="""Research the hollow Earth theory, its origins, and main claims. 
  Also, gather information about the current scientific understanding of Earth's structure. 
  Provide a comprehensive report on both aspects.""",
  agent=researcher,
  expected_output="A comprehensive report detailing the hollow Earth theory and current scientific understanding of Earth's structure."
)

task2 = Task(
  description="""Analyze the information gathered about the hollow Earth theory and scientific evidence. 
  Compare and contrast the claims of the theory with established scientific knowledge. 
  Provide a detailed analysis report, including the validity of the theory based on current scientific understanding.""",
  agent=analyst,
  expected_output="A detailed analysis report comparing the hollow Earth theory with established scientific knowledge, including an assessment of the theory's validity."
)

# Instantiate your crew with a sequential process
crew = Crew(
  agents=[researcher, analyst],
  tasks=[task1, task2],
  verbose=2,
  process=Process.sequential
)

# Get your crew to work!
result = crew.kickoff()

print("######################")
print(result)
