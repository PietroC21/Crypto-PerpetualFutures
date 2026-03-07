# Note
The following are announcements/emails from the professor & TA giving guidance on how to complete the assignments.

The pdfs referenced by them are in the "Guides" folder.

The professor and TA like to see lots of graphs, so include them wherever possible, you can see from previous homeworks how I have done this.

# Homework and Final Project Expectations (Professort Announcement)
Homework and final project submissions should generally read like a mini-academic paper, telling a "story" of your research.  

## Example of Great Communication
Consider for example the white paper below, with clear motivation, explanations of what is being analyzed, informative and well-labeled graphics, and so on.  It is definitely overkill for homework, but would be an excellent final project had it been submitted as such.  We will have more to say on final projects later in the quarter.

RavenPack-FXTradingOnNews.pdf 

## Homework Commentary
In homework, you should be much more terse, but I still expect a few sentences on motivation, observations/interpretations on results, explanations of any counterintuitive findings, and speculation you may have on the meaning of what you found and how the ideas could be extended.  In places where you have made subjective decisions, say about something like a lookback period, you should highlight that fact and (where appropriate) provide a sentence or two motivating your choice.

Compose your homework as though you are communicating with skilled colleagues who work abroad, and will need to understand your work without the convenience of a direct Zoom call or any other real-time medium.   Convey your ideas but do not be wordy.  It is very common when using LLMs to allow verbosity to creep in.  Your future colleagues (and our instructional team) will tolerate logorrhea poorly.

You typically do not need to recapitulate formulas and prose from the lecture notes.  (We will not reduce your grade if you include those things for your own reasons).  Sometimes, you may have a clearer story if you include a few formulas.

In grading homework, a minimum baseline is correctness in the source code.  It is convenient for the reader if you place functions, especially long functions, inside a subheading at the top, so that we can collapse them when we review the substance of your work.

A good approach to any HW set is to begin by writing some prose stating what you are investigating, and a rough plan for how you will do it.

Next, prototype your framework for your overall approach to the HW.  By this I mean deciding what function calls you reckon you need.  For example, 

def get_dividend_data(tickers, start_date, end_date):

    pass

def select_tickers_with_special_dividends(all_tickers, dividend_data):

    pass

...

def simulate_strategy_with_threshold(threshold, price_data, dividend_model_fits, tickers_selected, start_date, end_date):

    pass
Once you have done this, you can start to figure out what the code inside those functions should be, and whether maybe you need to alter them a bit, or add new ones.  Write the code inside the data fetching functions first and plot some things to make sure everything is sensible.

Run and debug your code on smaller data subsets at first (unless it is already very fast).  Once everything is working well, then worry about running full analyses.

Having finished running the code to generate analytic output, you can draw conclusions and complete the narrative.

## Grading
Our grading is deliberately set on an unusual scale -- we seek to have a median score for every HW set near 75%.  This gives us room to distinguish truly excellent work, of which we happily have many examples.

Below is an anonymized and somewhat altered example of a HW set that had various problems.  My observations about it include:

- Student identifiers (name, id) not included in file name
- Student identifiers (name, id) not included in file contents
- Functions and some constants appear at the top.  Good job!
- No section headings
- Plot has title but the axes are unlabeled
- Unexplained choice of a.  Role of these single-letter variables unstated.
- No summary of the assignment goals
- No interpretation of regression results into prose
- No explanation of what cells are trying to do or represent
- Window size explicitly stated as such.  Good!
- Should have more plots visually representing regression performance.  Should try to use grammar of graphics where appropriate.
- Ugly printed results rather than data frames
- No conclusion

In a final project, I would also have a problem with:
- Numbers printed to more digits of precision than is sensible



# Homework Grading (TA Email)
Hello everyone,

As homework grades start to come out, some of you may feel some disappointment, but this is in line with grading for this course.  Following all of the requirements correctly and submitting a bare minimum homework with properly formatted plots will earn you around 80-85 points.  Earning more points after that comes from polish, clarity, deeper analysis, and market, economic, or implementation considerations around the project.

A couple of extra comments I want to add regarding submissions:  One notebook should contain all relevant code, plots, and commentary.  You may use personal libraries or utility scripts, but the bulk of your work should be in this notebook.  The notebooks should be submitted with all output showing in the notebook, we shouldn't be needing to run your code for you.  Also, please try your best to suppress all warnings and exception messages to make the notebooks more readable.

Thanks,
Max

# How to Build a notebook:
- Using lots of graphs and having a short observation passage for any graph output
- Having a summary of the work completed and findings at the end of every section
- Conducting some extra experiments, only if they are directly relevant.
- Have a clear conclusion at the end with key findings and commentS.
- At all times I am aiming to have the highest grade in the class, so around a 96.

# Previous Homeworks
I received my grades for the following:

- Homework 1: 94 (top 1% of class)
Comment received from grader for this homeowork:
Good analysis and idiosyncratic exploration.

- Homework 2: 89 (Top 5% of class)
Comment received from grader for this homeowork:
Excellent work on this spread trading assignment. Your data pipeline correctly loads and regularizes ETH-USDT prices from all three exchanges. The strategy implementation is thorough with clear entry/exit band logic, stop-loss with cooldown, proportional trading costs, and mark-to-market capital enforcement. Your parameter sweep across j, g, ℓ, ζ, N, M is comprehensive with good visualization of Sharpe ratios and drawdowns. Strong additional work on regime analysis and robustness checks. Well-organized notebook with clear summaries.

- Homework 3: 96 (1st in class) Comment received from grader for this homeowork:
Excellent and very clean submission with a careful universe construction and well-implemented strategy. The analysis is thorough, clearly presented and the extra experiments are relevant