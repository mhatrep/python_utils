
import torch
import torch.nn as nn
from torch.nn import functional as F

# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------
torch.manual_seed(42)

device = "cuda" if torch.cuda.is_available() else "cpu"

batch_size = 16
block_size = 64       # Maximum context length
max_iters = 800       # Number of training iterations
eval_interval = 200
learning_rate = 1e-3

n_embd = 64            # Embedding dimension
n_head = 4             # Attention heads
n_layer = 2            # Transformer blocks
dropout = 0.0

# --------------------------------------------------
# 2. Training corpus
# Replace this with your own text later.
# --------------------------------------------------
text = """
A company reports revenue and expenses.
Revenue increased during the financial year.
Operating expenses reduced net income.
The balance sheet reports assets and liabilities.
Assets equal liabilities plus shareholders equity.
Cash is an important current asset.
Accounts receivable represents money owed by customers.
Accounts payable represents money owed to suppliers.
A tax statement reports taxable income and deductions.
Net income is revenue minus expenses.
Financial analysts compare companies across industries.
A real estate investment trust owns income producing property.
Rental income and operating costs affect property returns.
The company prepares an income statement and balance sheet.
Revenue revenue revenue income income assets liabilities.
The balance sheet reports assets and liabilities.
Revenue increased during the financial year.
Net income is revenue minus expenses.
"""

# --------------------------------------------------
# 3. Character-level tokenizer
# --------------------------------------------------
chars = sorted(set(text))
vocab_size = len(chars)

stoi = {ch: i for i, ch in enumerate(chars)}
itos = {i: ch for ch, i in stoi.items()}

encode = lambda s: [stoi[c] for c in s if c in stoi]
decode = lambda ids: "".join(itos[i] for i in ids)

data = torch.tensor(encode(text), dtype=torch.long)

if len(data) < 100:
    raise ValueError("Add more training text to the corpus.")

split = int(0.9 * len(data))
train_data = data[:split]
val_data = data[split:]

def get_batch(split_name):
    source = train_data if split_name == "train" else val_data
    ix = torch.randint(
        len(source) - block_size - 1, (batch_size,)
    )
    x = torch.stack([source[i:i+block_size] for i in ix])
    y = torch.stack([source[i+1:i+block_size+1] for i in ix])
    return x.to(device), y.to(device)

# --------------------------------------------------
# 4. Self-attention: learn which earlier characters
#    matter for predicting the next character.
# --------------------------------------------------
class Head(nn.Module):
    def __init__(self, head_size):
        super().__init__()
        self.key = nn.Linear(n_embd, head_size, bias=False)
        self.query = nn.Linear(n_embd, head_size, bias=False)
        self.value = nn.Linear(n_embd, head_size, bias=False)

        self.register_buffer(
            "mask", torch.tril(torch.ones(block_size, block_size))
        )

    def forward(self, x):
        B, T, C = x.shape
        k = self.key(x)
        q = self.query(x)

        weights = q @ k.transpose(-2, -1)
        weights = weights * (k.shape[-1] ** -0.5)
        weights = weights.masked_fill(
            self.mask[:T, :T] == 0, float("-inf")
        )
        weights = F.softmax(weights, dim=-1)

        v = self.value(x)
        return weights @ v


class MultiHeadAttention(nn.Module):
    def __init__(self):
        super().__init__()
        head_size = n_embd // n_head
        self.heads = nn.ModuleList(
            [Head(head_size) for _ in range(n_head)]
        )
        self.proj = nn.Linear(n_embd, n_embd)

    def forward(self, x):
        out = torch.cat([h(x) for h in self.heads], dim=-1)
        return self.proj(out)


# --------------------------------------------------
# 5. Feed-forward network and transformer block
# --------------------------------------------------
class FeedForward(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_embd, 4 * n_embd),
            nn.ReLU(),
            nn.Linear(4 * n_embd, n_embd),
        )

    def forward(self, x):
        return self.net(x)


class Block(nn.Module):
    def __init__(self):
        super().__init__()
        self.attention = MultiHeadAttention()
        self.ff = FeedForward()
        self.ln1 = nn.LayerNorm(n_embd)
        self.ln2 = nn.LayerNorm(n_embd)

    def forward(self, x):
        x = x + self.attention(self.ln1(x))
        x = x + self.ff(self.ln2(x))
        return x


# --------------------------------------------------
# 6. GPT language model
# --------------------------------------------------
class MiniGPT(nn.Module):
    def __init__(self):
        super().__init__()
        self.token_embedding = nn.Embedding(vocab_size, n_embd)
        self.position_embedding = nn.Embedding(block_size, n_embd)
        self.blocks = nn.Sequential(*[Block() for _ in range(n_layer)])
        self.ln_f = nn.LayerNorm(n_embd)
        self.lm_head = nn.Linear(n_embd, vocab_size)

    def forward(self, idx, targets=None):
        B, T = idx.shape

        token_emb = self.token_embedding(idx)
        positions = torch.arange(T, device=device)
        pos_emb = self.position_embedding(positions)

        x = token_emb + pos_emb
        x = self.blocks(x)
        logits = self.lm_head(self.ln_f(x))

        loss = None
        if targets is not None:
            loss = F.cross_entropy(
                logits.reshape(-1, vocab_size),
                targets.reshape(-1)
            )
        return logits, loss

    @torch.no_grad()
    def generate(self, idx, max_new_tokens=250):
        for _ in range(max_new_tokens):
            idx_cond = idx[:, -block_size:]
            logits, _ = self(idx_cond)
            logits = logits[:, -1, :]
            probs = F.softmax(logits, dim=-1)
            next_id = torch.multinomial(probs, num_samples=1)
            idx = torch.cat((idx, next_id), dim=1)
        return idx


# --------------------------------------------------
# 7. Train the model
# --------------------------------------------------
model = MiniGPT().to(device)
optimizer = torch.optim.AdamW(
    model.parameters(), lr=learning_rate
)

def estimate_loss():
    model.eval()
    results = {}
    with torch.no_grad():
        for name in ["train", "val"]:
            losses = []
            for _ in range(10):
                xb, yb = get_batch(name)
                _, loss = model(xb, yb)
                losses.append(loss.item())
            results[name] = sum(losses) / len(losses)
    model.train()
    return results

print(f"Device: {device}")
print(f"Vocabulary size: {vocab_size} characters")
print(f"Training characters: {len(train_data)}")
print(f"Parameters: {sum(p.numel() for p in model.parameters()):,}")

model.train()
for step in range(max_iters):
    if step % eval_interval == 0:
        print(step, estimate_loss())

    xb, yb = get_batch("train")
    _, loss = model(xb, yb)

    optimizer.zero_grad(set_to_none=True)
    loss.backward()
    optimizer.step()

print("Final loss:", estimate_loss())
print("Training complete!")

# --------------------------------------------------
# 8. Generate text
# --------------------------------------------------
prompt = "The balance sheet"
context = torch.tensor([encode(prompt)], dtype=torch.long).to(device)

if context.shape[1] == 0:
    context = torch.zeros((1, 1), dtype=torch.long, device=device)

generated = model.generate(context, max_new_tokens=300)
print("\n--- GENERATED TEXT ---")
print(decode(generated[0].tolist()))

# --------------------------------------------------
# 9. Save the trained model
# --------------------------------------------------
torch.save(model.state_dict(), "mini_gpt_weights.pt")
print("Saved weights to mini_gpt_weights.pt")
