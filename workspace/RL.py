import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np

class SimpleEnvironment:
  def __init__(self, sequence_length=10):
    self.sequence_length = sequence_length

  def reset(self):
    # Start with a random initial state
    self.state = np.random.randint(0, 100, size=self.sequence_length)
    return self.state

  def step(self, actions):
    # The reward is simply the sum of the actions
    reward = np.sum(actions)
    # The new state can be anything; for simplicity, let's keep it unchanged
    next_state = self.state
    done = True  # End after one sequence
    return next_state, reward, done

class PolicyNetwork(nn.Module):
  def __init__(self, input_size, hidden_size, output_size):
    super(PolicyNetwork, self).__init__()
    self.fc1 = nn.Linear(input_size, hidden_size)
    self.fc2 = nn.Linear(hidden_size, hidden_size)
    self.fc3 = nn.Linear(hidden_size, output_size)

  def forward(self, x):
    x = torch.relu(self.fc1(x))
    x = torch.relu(self.fc2(x))
    x = torch.softmax(self.fc3(x), dim=-1)
    return x

def train(env, policy_network, optimizer, num_episodes=1000):
  for episode in range(num_episodes):
    state = env.reset()
    state = torch.FloatTensor(state)

    # Get the action probabilities from the policy network
    action_probs = policy_network(state)
    action_distribution = torch.distributions.Categorical(action_probs)
    actions = action_distribution.sample()  # Sample an action sequence
    
    # Convert the actions to numpy and pass to the environment
    actions_np = actions.detach().numpy()
    _, reward, done = env.step(actions_np)

    # Compute the loss and perform backpropagation
    loss = -torch.sum(action_distribution.log_prob(actions)) * reward
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    if episode % 100 == 0:
      print(f"Episode {episode}, Loss: {loss.item()}, Reward: {reward}")

# Hyperparameters
input_size = 10  # Length of the state sequence
hidden_size = 64
output_size = 10  # Length of the action sequence
learning_rate = 0.01

# Create environment and policy network
env = SimpleEnvironment(sequence_length=input_size)
policy_network = PolicyNetwork(input_size=input_size, hidden_size=hidden_size, output_size=output_size)

# Optimizer
optimizer = optim.Adam(policy_network.parameters(), lr=learning_rate)

# Train the policy network
train(env, policy_network, optimizer)
