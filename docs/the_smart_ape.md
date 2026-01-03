这是一条来自 **@the_smart_ape** 的推文（或推文串），主要内容是关于构建和回测一个 Polymarket 交易机器人的技术细节。

以下是根据该推文内容整理的 Markdown 格式输出：

---

# I built a Polymarket bot and tested multiple parameter setups, here are the results.

**Author:** @the_smart_ape

A few weeks ago, I decided to build my own Polymarket bot. The full version took me several weeks to complete. I was willing to put in this effort because there is indeed an efficiency gap on Polymarket.

Although some bots in the market are already taking advantage of these inefficiencies, it is still far from enough, and the opportunities in this market are still far greater than the number of bots.

## Bot Building Logic

The bot's logic is based on a set of strategies I have manually executed in the past, which I automated to improve efficiency. The bot runs on the **"BTC 15-minute UP/DOWN"** market.

The bot runs a real-time monitoring program that can automatically switch to the current BTC 15-minute round, streamlining the best bid/ask through a WebSocket, displaying a fixed terminal UI, and allowing comprehensive control through text commands.

### Manual Mode

In manual mode, you can place orders directly:

* `buy up <usd>` / `buy down <usd>`: Buy a specific amount in USD.
* `buyshares up <shares>` / `buyshares down <shares>`: Purchase an exact amount of shares, using a user-friendly LIMIT + GTC (Good 'Til Canceled) order, executed at the current best ask price.

### Automatic Mode

Automatic mode runs a recurring **two-leg loop**:

1. **Leg 1**: It only observes price movements within the `windowMin` minutes at the start of each round. If either side drops fast enough (reaching a drop percentage of at least `movePct` in about 3 seconds), it triggers "Leg 1," buying the side that experienced the sharp decline.
2. **Leg 2 (Hedge)**: After completing Leg 1, the bot will never again buy the same side. It will wait for the "Second Leg" and only trigger if the following condition is met:
* `leg1EntryPrice + oppositeAsk <= sumTarget`
* When this condition is met, it buys the opposite side.



After Leg 2 is completed, the cycle ends, the bot returns to watching mode, waiting for the next flash crash signal using the same parameters. If there is a change in the round during the cycle, the bot abandons the open cycle and restarts with the same settings in the next round.

**Parameter settings for auto mode:**
`auto on <shares> [sum=0.95] [move=0.15] [windowMin=2]`

* **shares**: Position size for the two-stage trade.
* **sum**: Threshold for allowed hedging.
* **move (movePct)**: Flash crash threshold (e.g., 0.15 = 15%).
* **windowMin**: Time from the start of each round to allow the execution of Leg 1.

## Backtesting

The bot's logic is simple: wait for a violent flash crash, buy the side that just dropped, then wait for the price to stabilize and hedge by buying the opposite side, ensuring that `priceUP + priceDOWN < 1`.

But this logic needs to be tested. Is it really effective in the long run? More importantly, the bot has many parameters. Which parameter set is optimal and maximizes profit?

### The Data Problem

My first thought is to backtest using online historical data from the Polymarket CLOB API. Unfortunately, for the BTC 15-minute up/down market, the historical data endpoint consistently returns empty datasets.

Due to this limitation, the only reliable way to backtest this strategy is to create my own historical dataset by recording real-time best-ask prices while the bot is running.
The recorder will write snapshots to disk, including: Timestamp, Round Slug, Remaining Seconds, UP/DOWN Token ID, UP/DOWN Best Ask Price.

Over 4 days, I collected a total of **6 GB of data**.

### Results

**Test 1 (Conservative):**

* Initial Balance: $1,000
* 20 Shares per Trade
* sumTarget = 0.95
* Flash Crash Threshold = 15%
* windowMin = 2 minutes
* Fee: 0.5%, Spread: 2%

**Outcome:** The backtest showed an **86% ROI**, turning $1,000 into $1,869 in just a few days.

**Test 2 (Aggressive):**

* Initial Balance: $1,000
* 20 Shares per Trade
* sumTarget = 0.6
* Flash Crash Threshold = 1%
* windowMin = 15 minutes

**Outcome:** After 2 days, the investment had a return rate of **-50%**.

This clearly demonstrates that **parameter selection is the most critical factor**. It can make you a lot of money or lead to significant losses.

## Limitations of Backtesting

Even with costs and spreads included, backtesting has its limitations:

1. **Limited Data**: Only uses a few days' worth of data.
2. **Fill Assumptions**: Relies on recorded optimal sell price snapshots; doesn't model order book depth or partial fills.
3. **Micro-fluctuations**: Sub-second fluctuations are not captured.
4. **Network Latency**: Slippage is constant without simulating variable delays.
5. **Market Impact**: Backtesting assumes you are a pure liquidity extractor (price taker) with no influence on the order book.
6. **Real-world Errors**: Does not simulate rate limits, API errors, or timeouts.

To maintain a pessimistic approach, I applied a rule: *if Leg 2 fails to execute before the market close, Leg 1 is considered a total loss.*

## Infrastructure

I plan to run this bot on a **Raspberry Pi** to avoid consuming resources on my main machine and keep it running 24/7.

**Future Improvements:**

* Using **Rust** instead of JavaScript for better performance.
* Running a dedicated **Polygon RPC node** to reduce latency.
* Deploying on a **VPS** close to the Polymarket server.