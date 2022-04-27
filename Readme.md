### What

Toy transaction processing engine. 

#### Build 

`cargo build --release`

#### Run 

`cargo run --release -- $INFILE.csv`

#### Docs 

`cargo doc --open` 

#### Tests 

`cargo test`

### Implementation details 

#### Assumptions made

- Only Deposits can be disputed. Evaluating sample withdrawal dispute scenarios led to conflicts with other requirements (either available funds increase before chargeback, or clients account ends up being charged twice).
- No checks for duplicate transaction ids - based on the statement that all transaction ids are globally unique, there is no check for deposit with same id. This means that in the event of two deposits with different amounts but same transaction id and same client id, later transaction would overwrite previous. 

Each clients balance is managed by a lightweight task. Compared to single loop of `read line > parse > apply to state` this approach allows for horizontal scaling, (i.e. opens a possibility for client-specific task to be migrated to a different host). 

#### Components 

1. Parser: reads csv and handles deserialization

[![](https://mermaid.ink/img/pako:eNo1j81ug0AMhF9l5ROVkhfg0B4KBw40FeRSaS8Wa5JVl3VklvQHePeaipw8M_6skWfo2BHk0Af-6q4oyZwLY2OTNYTOBB_pycYiK2gk8Rj8L5mGOhancZ29cryT3iQ2NY0jXja6zVqKbsvehTuNWTSt5pqFjI-3Kb2sNpZZ-e2TLrTseHwutEXHUoos1a5Pn0utLbtsza7_ETXVZj5oXJqHeeOlhAMMJAN6p1_NNhpjIV1pIAu5Skc9TiFZsHFVdLo5TFQ6n1gg7zGMdACcErc_sYM8yUQPqPB4ERx2av0DTABryQ)](https://mermaid.live/edit#pako:eNo1j81ug0AMhF9l5ROVkhfg0B4KBw40FeRSaS8Wa5JVl3VklvQHePeaipw8M_6skWfo2BHk0Af-6q4oyZwLY2OTNYTOBB_pycYiK2gk8Rj8L5mGOhancZ29cryT3iQ2NY0jXja6zVqKbsvehTuNWTSt5pqFjI-3Kb2sNpZZ-e2TLrTseHwutEXHUoos1a5Pn0utLbtsza7_ETXVZj5oXJqHeeOlhAMMJAN6p1_NNhpjIV1pIAu5Skc9TiFZsHFVdLo5TFQ6n1gg7zGMdACcErc_sYM8yUQPqPB4ERx2av0DTABryQ)

<details>
<summary> Mermaid for parser </summary>

```txt
flowchart TD 
R(Read line)
D(Deserialize Record)
M(Convert to Message)
S(Send to Processor)
I{More input?}
E(Exit)

R-->D
D-->|Err|I
D-->|Ok|M
M-->|Ok|S 
M-->|Err|I 
I-->|Yes|R 
I-->|No|E
```
</details>


2. Processor: reads parsed and validated messages from parser, spawns account tasks for new clients, routes transactions to clients. 

[![](https://mermaid.ink/img/pako:eNpdkDFPwzAQhf-K5aVBKrBHUIRI0nZIh4QFyYuJL63V2FfZZwXU9L9jlyAQ8vL87vPz3Z15hwp4zvsBx-4gHbHXggnbZA1IxWrwXu7hRtjtuUYHTNtToKeLsOtNtgZiB2nVAKxHx7pBgyWmI1zVWYVulO5vQpu1lPKfuw5DBEn648O7u1_9f11kL2hMsLqTBOwbWeyQmdTAiO64mE1C5k9ytKCuYT6-LbPyQ1MU6TS3t6vJ-P3dHP7I9LTepN5TYYcUfw5WTW0cuI3Wb626-lWdRon3bQS2yX8DPzWz3uGUNlVEXfIlN-CM1Cqu8iwsY4LTAQwInkepoJdhIMGFvUQ0nFScrFSa0PG8l4OHJZeBsP20Hc_JBfiBCi33TpqZunwBJr6SXA)](https://mermaid.live/edit#pako:eNpdkDFPwzAQhf-K5aVBKrBHUIRI0nZIh4QFyYuJL63V2FfZZwXU9L9jlyAQ8vL87vPz3Z15hwp4zvsBx-4gHbHXggnbZA1IxWrwXu7hRtjtuUYHTNtToKeLsOtNtgZiB2nVAKxHx7pBgyWmI1zVWYVulO5vQpu1lPKfuw5DBEn648O7u1_9f11kL2hMsLqTBOwbWeyQmdTAiO64mE1C5k9ytKCuYT6-LbPyQ1MU6TS3t6vJ-P3dHP7I9LTepN5TYYcUfw5WTW0cuI3Wb626-lWdRon3bQS2yX8DPzWz3uGUNlVEXfIlN-CM1Cqu8iwsY4LTAQwInkepoJdhIMGFvUQ0nFScrFSa0PG8l4OHJZeBsP20Hc_JBfiBCi33TpqZunwBJr6SXA)


<details>
<summary> Mermaid for Processor </summary>

```txt
flowchart TD 
R(Read Message)
I{More input?}
GH(Get handle for client i)
FM(Forward Message)
S(Start Account task<br/> for client i)
D(Communicate <br/> 'No more work' <br/> to spawned tasks)
E(Exit)


R-->|msg.client = i|GH
GH-->|Not found|S 
S-->GH
GH-->|Found|FM
FM-->I 
I-->|Yes|R
I-->|No|D 
D-->E 
```
</details>

3. Account task: applies valid messages forwarded by processor to the internal state of the client account. 

[![](https://mermaid.ink/img/pako:eNo1TsuqwjAQ_ZUwqwr6A10ohXbRhXdRBRGyGZqpBtokJBNU2v67o3hX5zDnNTP03hCUMIz-0d8xsjrXSruu6AiNOlJKeKONdu189JGUdSHzYdWuKqoQxpdirxIjfyy1ZIKXhu_ho1yiZYoiNUXztCxEmne7fSV5gVaGWsHlSmnpfvzPL7V0CW1gCxPFCa2RB2ftlNLAd5pIQynU0IB5ZA3arWLNwchsYyz7COWAY6ItYGZ_erkeSo6Z_k21xVvE6eda327bWP4)](https://mermaid.live/edit#pako:eNo1TsuqwjAQ_ZUwqwr6A10ohXbRhXdRBRGyGZqpBtokJBNU2v67o3hX5zDnNTP03hCUMIz-0d8xsjrXSruu6AiNOlJKeKONdu189JGUdSHzYdWuKqoQxpdirxIjfyy1ZIKXhu_ho1yiZYoiNUXztCxEmne7fSV5gVaGWsHlSmnpfvzPL7V0CW1gCxPFCa2RB2ftlNLAd5pIQynU0IB5ZA3arWLNwchsYyz7COWAY6ItYGZ_erkeSo6Z_k21xVvE6eda327bWP4)

<details> 
<summary> Mermaid for account task </summary>

```txt
flowchart TD 
R(Read Message)
I{More input?}
A(Apply to state)
D(Report state to Writer)
E(Exit)

R-->A
A-->I 
I-->|Yes|R
I-->|No|D
D-->E
```
</details>


4. Writer: collects account states from account tasks and writes to stdout as csv. 

[![](https://mermaid.ink/img/pako:eNo1zsEKgzAMBuBXKTk5cC_gYbvowcN2cAMZ9BJsdAVtpU3Zhvruy2Ce_j_wJWSBzhuCAvrRv7onBlb3UmnXZA2hUReKEQc6aFcvFx9IWTcnPm_atVkbLJNiryIbn1hMlVVv-yuyfzyeWlEStZyrJdcHxbXZh6tfK8hhojChNfLAop1SGvhJE2kopBrqMY2sQbtNaJoNMlXGsg9Q9DhGygET-9vHdVBwSLSj0uIQcPqr7Qvofk5B)](https://mermaid.live/edit#pako:eNo1zsEKgzAMBuBXKTk5cC_gYbvowcN2cAMZ9BJsdAVtpU3Zhvruy2Ce_j_wJWSBzhuCAvrRv7onBlb3UmnXZA2hUReKEQc6aFcvFx9IWTcnPm_atVkbLJNiryIbn1hMlVVv-yuyfzyeWlEStZyrJdcHxbXZh6tfK8hhojChNfLAop1SGvhJE2kopBrqMY2sQbtNaJoNMlXGsg9Q9DhGygET-9vHdVBwSLSj0uIQcPqr7Qvofk5B)

<details>
<summary> Mermaid for Writer </summary>

```txt 
flowchart TD 
R(Read Message)
I{More input?}
W(Write to stdout)
E(Exit)

R-->W
W-->I 
I-->|Yes|R 
I-->|No|E
```
