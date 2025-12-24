from perplexity import Perplexity
import os 
from dotenv import load_dotenv

class PerplexityAssistant():
    
    def __init__(self):
        load_dotenv()
        self.client = Perplexity(api_key=os.getenv("PERPLEXITY_API_KEY"))
        
        
        self.system_prompt = """
        Your are a knowledgable assistant that 
        - Searches for real time engines
        - Provide accurate, real data
        - Admits when information is uncertain
        - Summarize the information 
        - After the summarizatio print's Final word about market Good, Bad, Neutral
        - Use final words about overall market sentiment Good, Bad, Neutral
        - Use word which can be useful to scrape Upercase words 
        - Add sort term long term mid term but use todays overall market sentiment just upercase words
        - Always use same wording and last sentence will be TODAY OVERALL MARKET SENTIMENT: NEUTRAL,GOOD,BAD
        """
    def Ask(self,question):
        instructions = self.system_prompt
        
        messages = [
            {"role": "system", "content":instructions},
            {"role": "user", "content": question}
        ]
        completion = self.client.chat.completions.create(
            messages = messages,
            model = "sonar-pro"
        )
        return completion.choices[0].message.content
    
assistant = PerplexityAssistant()

response = assistant.Ask("what's happening Crypto market today")


def Get_Last_Sentiment(response):
    lines = response.split('\n')
    last_line = lines[-1]
    last_one = last_line.split()[-1]
    string_replace = last_one.replace(',', '').replace('*', '').replace('-', '').replace('&', '')
    return string_replace


result= Get_Last_Sentiment(response=response)
print(result)