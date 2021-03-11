from numpy import random, prod
from math import floor, sqrt
import csv
import time

import os
# import errno

from os import listdir
from os.path import isfile, join as pathjoin, abspath
# import glob
import json
import matplotlib.pyplot as plt


"""
the following is for the error graphs. y1 and y2 are the errors for bernolli and
correlated resp. See my_xticks for the percentages. You can change them if needed.
CONTROL + / to uncomment
"""
# x = np.array([1,2,3,4,5,6])
# my_xticks = ['0.01%', '0.03%', '0.05%', '0.1%', '1%', '3%']
# plt.figure(figsize=(10,10))
#
# y1 = np.array([3.4546873550463464, 1.2528104311849029, 0.6953863466523523, 0.4189249438698155, 0.04416772281066896, 0.021172714989670317])
# y2 = np.array([0.2713194448469827, 0.1750384856139565, 0.12776239817121424, 0.09567697890223587, 0.02768731570812195, 0.01790332499761598])
# plt.ylim([0,2])
# plt.plot(x,y1, label='Bernoulli')
# plt.plot(x,y2, label = 'Correlated')
# plt.legend(loc='best')
# plt.xticks(x, my_xticks)
#
#
# plt.show()


"""
the following is for histograms
bins(start of x_axis, end of x_axis, size of bin)
ax.set_xticks(list of ticks on x axis. I recommend beginning middle and end
of the appropriate range
for consistency
"""
outputs = [7241129, 7728829, 7208029, 7725229, 7289829, 7191729, 7750929, 7386429, 7403529, 7039229, 7288629, 7817429, 7287829, 7351929, 7339429, 7504529, 7247529, 7437029, 7291929, 7509029, 7637529, 7487629, 7293429, 7666529, 7090229, 7226729, 7494829, 7603129, 7404129, 7560029, 7535429, 7236229, 7460929, 7516029, 7529029, 7645029, 7764729, 6881429, 7764429, 7392929, 7571529, 7482929, 7270329, 7284629, 7379329, 7505729, 7472729, 7001529, 7714229, 7275529, 7111629, 6936229, 7615829, 7370429, 7166529, 7444429, 7256029, 7721029, 7436829, 7438829, 7293829, 7670629, 7265529, 7394929, 7421529, 7437429, 7462329, 7260829, 6938129, 7503329, 7428729, 7369329, 7543629, 7544529, 7043229, 6953929, 7377329, 7309529, 7445329, 7080529, 7280429, 7310329, 7282529, 7167229, 7460029, 7637829, 7023329, 7206729, 7622229, 7596829, 7505329, 7799829, 7312029, 7438529, 7269329, 7077729, 7095229, 7397929, 7440829, 7280229, 7448229, 7270029, 7494529, 7534529, 7663429, 7217829, 7405529, 7228429, 7559029, 7074129, 7535229, 7272229, 7385429, 7773929, 7537529, 7470529, 7466429, 7715229, 7165029, 7208629, 7686629, 7697029, 7505929, 7316129, 7400629, 7590729, 7571129, 7030029, 7366029, 7522029, 7326329, 7474829, 7128129, 7725429, 7309229, 7644329, 7256229, 7266829, 7534229, 7167629, 7536329, 7063329, 7339729, 7242929, 7082829, 7376029, 7563629, 7580129, 7431629, 7468629, 7205229, 7520629, 7364829, 7711229, 7663029, 7267029, 7371629, 7118829, 6899129, 7288829, 7617329, 7280129, 7563529, 7765229, 7782329, 7502329, 7476829, 7304929, 6998129, 7281129, 7549429, 7646029, 7588329, 7083829, 7394729, 7543229, 7514029, 7342929, 7341429, 7493629, 7376829, 7252629, 7886529, 7355129, 7541029, 7700029, 7347029, 7261929, 7016029, 7409529, 7274529, 7348829, 7535129, 7538929, 7190229, 7311429, 7551429, 7237629, 7534829, 7597829, 7527829, 7484729, 7361829, 7289029, 7291029, 7392629, 7354629, 7446829, 7722529, 7396829, 7504029, 7488129, 7346829, 7748829, 7371929, 7405629, 7181229, 7667629, 7361129, 7613829, 7422329, 7260429, 7554329, 7738429, 7376929, 7285629, 7193429, 7243129, 7334529, 7158829, 7641829, 7182729, 7186829, 7399529, 7491929, 7127729, 7667429, 7669029, 7284129, 7667829, 7682729, 7778929, 7531329, 7101329, 7339029, 7381729, 7576429, 7812229, 7686029, 7552629, 7555929, 7553929, 6902629, 7648329, 7225629, 7511729, 7200829, 7372729, 7195729, 7816729, 7355729, 7230729, 7274729, 7461929, 7424629, 7238829, 7369929, 7376629, 7158929, 7427829, 7495529, 7329029, 7104129, 7401229, 7144229, 7336429, 7835529, 7263429, 7681529, 7113629, 7335829, 7503029, 7276329, 7459529, 7548929, 7438229, 7544529, 7783329, 7411529, 7569129, 7400629, 7450329, 7339029, 7372929, 7096029, 7485029, 7536829, 7588329, 7202429, 7340929, 7243429, 7498329, 7589229, 7650429, 7494429, 7656829, 7868229, 7363329, 7706529, 7561629, 7555729, 7667329, 7476329, 7346429, 7687229, 8074029, 7324629, 7711829, 7051029, 7437129, 7505429, 7532129, 7168829, 7538129, 7335629, 7541129, 7515929, 7282829, 7657329, 7201129, 7569929, 7621929, 7366629, 7231529, 7433229, 7546329, 7469129, 7698929, 7291029, 7677329, 7415129, 7561629, 7500329, 7725329, 7412129, 7364929, 7685629, 7361729, 7318929, 7141829, 7565029, 7626329, 7511129, 7590929, 7551129, 7629629, 7256829, 7370129, 7695329, 7295229, 7189729, 7212929, 7502029, 7566029, 7363829, 7355429, 7565829, 7247029, 7078129, 7212329, 7504129, 7285229, 7548929, 7396329, 6985029, 7571429, 7071729, 7196829, 7285929, 7476329, 7375329, 7398529, 7486129, 7269529, 7718529, 7591329, 7484129, 7600429, 7211629, 7509929, 7562829, 7614029, 7543629, 7546729, 7381029, 7699829, 7342329, 7336529, 7308829, 7457129, 7553829, 7785129, 7283929, 7153429, 7191929, 7515229, 7393329, 7815729, 7499029, 7691729, 7774129, 7314229, 7210029, 7200329, 7284529, 7473129, 7372929, 7638929, 7513529, 7540229, 7545929, 7422729, 7395829, 7570929, 7242429, 7504229, 7326429, 7898629, 7444229, 7246529, 7345329, 7471829, 7302129, 7286529, 7391429, 7192629, 7287229, 7362529, 7250629, 7774829, 7275229, 7660429, 7394829, 7314729, 7169929, 7291629, 7443829, 7585829, 7337329, 7637029, 7245829, 7294729, 7752829, 7052129, 7697929, 7163429, 7368029, 7482729, 7347229, 7570929, 7456829, 7215929, 7489029, 7576929, 7250529, 7585929, 7097629, 7323729, 7346029, 7337429, 7340729, 7676129, 7333429, 7446029, 7311729, 7047329, 7480029, 7533929, 7620629, 7641729, 7630429, 7184129, 7472729, 7123829, 7246829, 7481729, 7427329, 7005529, 7655829, 7070429, 7261529, 7557829, 7209929, 7757329, 7610429, 7596229, 7183529, 7303229, 7434929, 7229729]
bins = range(6000000,9000000,12000)
fig, ax = plt.subplots(figsize=(6, 4))
fig.suptitle('Correlated Sampling (no heavy hitters)')
ax.hist(outputs, bins)
ax.locator_params(axis='y', integer=True)
ax.set_yticks([])
ax.set_xticks([6000000,7500000,9000000])
plt.show()