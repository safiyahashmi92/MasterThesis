# Run with:
#   python3 lstm_vitals_files.py sequence_length sofa_threshold
#   python3 lstm_vitals_files.py 5 7 
#
#

# import libraries
import pandas as pd
import numpy as np
import sys
import tensorflow as tf

# Setting seed for reproducibility
np.random.seed(4)
#PYTHONHASHSEED = 0

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, recall_score, precision_score, balanced_accuracy_score, accuracy_score
from tensorflow.python.keras.models import Sequential, load_model


import keras
from keras.models import Sequential,load_model
from keras.layers import Dense, Dropout, LSTM


# function to generate sequences
def gen_sequence(id_df, seq_length, seq_cols):
    data_matrix = id_df[seq_cols].values
    # print(data_matrix)
    num_elements = data_matrix.shape[0]
    for start, stop in zip(range(0, num_elements - seq_length), range(seq_length, num_elements)):
        #print(start, stop)
        yield data_matrix[start:stop, :]


# function to generate labels
def gen_labels(id_df, seq_length, label):
    data_matrix = id_df[label].values
    # print(data_matrix)
    num_elements = data_matrix.shape[0]
    labels_array =  data_matrix[seq_length:num_elements, :]
    return labels_array

# function to read file, scale data and generate label based on the sofa_threshold
def read_data_add_labels(file_name, sofa_threshold, sequence_cols, sequence_cols_all):
    # https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html
    scaler = StandardScaler()
    train_df = pd.read_csv(file_name)
    # first scale the values we are using as features
    train_df[sequence_cols] = scaler.fit_transform(train_df[sequence_cols])
    # here I am adding a label based on the sofa_Score
    train_df['mylabel'] = np.where(train_df.sofa_Score > sofa_threshold, 'shock','Nonshock')
    train_df = train_df.loc[:,sequence_cols_all]
    ##### this does not work if my data has only one label, for example Nonshock
    #####label_encoding = pd.get_dummies(train_df.mylabel)
    # I need to check here if the data has just one label, which can be the case of Non-shock data
    if(len(train_df['mylabel'].value_counts())==2 ):
        label_encoding = pd.get_dummies(train_df.mylabel)
        train_df = pd.concat([train_df, label_encoding], axis=1)
    else:
        # TODO: improve!, here assuming that if we have only one label is because all is Nonshock
        train_df['Nonshock'] = 1
        train_df['shock'] = 0
    return train_df


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    print('arg[1]',sys.argv[1],' arg[2]', sys.argv[2])

    sequence_length = int(sys.argv[1])
    sofa_threshold = int(sys.argv[2])

    # pick the feature columns
    sequence_cols = ['hr', 'abpSys', 'abpDias', 'abpMean', 'resp', 'sp02', 'SDhr', 'SDabpSys', 'SDabpDias', 'SDabpMean', 'SDresp', 'SDsp02', 'SEhr', 'SEresp', 'CorHRabpSys', 'CorHRabpDias', 'CorHRabpMean', 'CorHRresp', 'CorHRsp02', 'CorRespSp02']
    sequence_cols_all = ['DateVitals', 'hr', 'abpSys', 'abpDias', 'abpMean', 'resp', 'sp02', 'SDhr', 'SDabpSys', 'SDabpDias', 'SDabpMean', 'SDresp', 'SDsp02', 'SEhr', 'SEresp', 'CorHRabpSys', 'CorHRabpDias', 'CorHRabpMean', 'CorHRresp', 'CorHRsp02', 'CorRespSp02', 'sofa_Score','mylabel']


    # generate train set
    path = '/projects/data/PhysioNet/mimic3wdb/matched/six_vs_cli_onset/'

    #-------------------------------    
    # Train data
    train_set = ['Non-shock-Patient_id-89734-vs-cli.csv','Shock-Patient_id-89091-vs-cli.csv','Non-shock-Patient_id-96350-vs-cli.csv','Shock-Patient_id-98006-vs-cli.csv', 'Non-shock-Patient_id-66152-vs-cli.csv','Shock-patient_id-69272-vs-cli.csv']
    seq_gen_train = []
    n=0;
    for file in train_set:
        file_path = path+file
        print("Processing: ",file_path)
        train_df = read_data_add_labels(file_path, sofa_threshold, sequence_cols, sequence_cols_all)
        print('\nLabels distribution file:', file)
        print(train_df['mylabel'].value_counts())
        
        # generate the sequences, of size sequence_length
        seq_gen = list(gen_sequence(train_df, sequence_length, sequence_cols))
        # generate labels
        label_gen = gen_labels(train_df, sequence_length, ['Nonshock', 'shock'])
        # concatenale seqs and labels
        seq_gen_train = seq_gen_train + seq_gen
        if(n==0):
            label_gen_train = label_gen
        else:
            label_gen_train = np.concatenate((label_gen_train,label_gen), axis=0)
        print('num sequences:', len(seq_gen), ' concatenated: ', len(seq_gen_train))
        print('num labels:', label_gen.shape, ' concatenated: ', label_gen_train.shape)
        n=n+1


    #-------------------------------
    # Test data
    test_set = ['Non-shock-Patient_id-68964-vs-cli.csv', 'Shock-Patient_id-71405-vs-cli.csv']
    seq_gen_test = []
    n=0;
    for file in test_set:
        file_path = path+file
        print("Processing: ",file_path)
        test_df = read_data_add_labels(file_path, sofa_threshold, sequence_cols, sequence_cols_all)
        print('\nLabels distribution file:', file)
        print(test_df['mylabel'].value_counts())
        
        # generate the sequences, of size sequence_length
        seq_gen = list(gen_sequence(test_df, sequence_length, sequence_cols))
        # generate labels
        label_gen = gen_labels(test_df, sequence_length, ['Nonshock', 'shock'])
        # concatenale seqs and labels
        seq_gen_test = seq_gen_test + seq_gen
        if(n==0):
            label_gen_test = label_gen
        else:
            label_gen_test = np.concatenate((label_gen_test,label_gen), axis=0)
        print('num sequences:', len(seq_gen), ' concatenated: ', len(seq_gen_test))
        print('num labels:', label_gen.shape, ' concatenated: ', label_gen_test.shape)
        n=n+1


    #--------------------------------    
    # create train data arrays
    seq_array_train = np.array(list(seq_gen_train)).astype(np.float32)
    print(seq_array_train.shape)
    label_array_train = np.array(label_gen_train).astype(np.float32)
    # print(label_array_train)
    print(label_array_train.shape)

    #--------------------------------    
    # create test data arrays
    seq_array_test = np.array(list(seq_gen_test)).astype(np.float32)
    print(seq_array_test.shape)
    label_array_test = np.array(label_gen_test).astype(np.float32)
    # print(label_array_test)
    print(label_array_test.shape)

    

    # -----
    # Create model
    # number of features
    nb_features = seq_array_train.shape[2]
    # number of classes
    nb_out = label_array_train.shape[1]

    # create model 1
    # model = vi.create_bi_model(nb_features, nb_out)
    model = Sequential()
    model.add(tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(units=600, input_shape=(nb_features, nb_out))))
    model.add(tf.keras.layers.Dropout(rate=0.2))
    model.add(tf.keras.layers.Dense(units=nb_out))
    model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])
    #model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])  # big diff between the two loss ???

#    # model 3
#    # from https://stackabuse.com/solving-sequence-problems-with-lstm-in-keras/
#    # https://stackabuse.com/solving-sequence-problems-with-lstm-in-keras-part-2/
#    model = Sequential()
#    model.add(tf.keras.layers.Bidirectional(LSTM(100, activation='relu', return_sequences=True, input_shape=(nb_features, nb_out))))
#    model.add(Dropout(0.2))
#    model.add(tf.keras.layers.Bidirectional(LSTM(50, activation='relu', return_sequences=True)))
#    model.add(Dropout(0.2))
#    model.add(tf.keras.layers.Bidirectional(LSTM(25, activation='relu')))
#    model.add(Dense(10, activation='relu'))
#    model.add(Dropout(0.2))
#    model.add(Dense(units=nb_out))
#    model.compile(optimizer='adam', loss='mse', metrics=['accuracy'])


    # model 2
    # this model from:
    # https://www.kaggle.com/vamshikreddy/predictive-maintenance-using-lstm/code
#    model = Sequential()
#    model.add(tf.keras.layers.Bidirectional(LSTM(input_shape=(nb_features, nb_out), units=600, return_sequences=True)))
#    model.add(Dropout(0.2))
#    model.add(tf.keras.layers.Bidirectional(LSTM(units=100, return_sequences=False)))
#    model.add(Dropout(0.2))
#    #model.add(tf.keras.layers.Bidirectional(LSTM(units=100, return_sequences=False)))
#    #model.add(Dropout(0.2))
#    model.add(Dense(units=nb_out))
#    #model.add(Dense(units=nb_out, activation='sigmoid'))
#    model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])
#    #model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
 
    
   # fit the network
    history = model.fit(seq_array_train, label_array_train, epochs=100, batch_size=50, verbose=2, shuffle=False
                        # ,validation_split=0.05,
                        # callbacks = [tf.keras.callbacks.EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=0, mode='min')]
                        # ,tf.keras.callbacks.ModelCheckpoint(model_path,monitor='val_loss', save_best_only=True, mode='min', verbose=0)]
                        )




    # list all data in history
    print(history.history.keys())

    scores = model.evaluate(seq_array_train, label_array_train, verbose=1, batch_size=16)
    print('\n----\nAccurracy: {}'.format(scores[1]))

    #------------------------
    # Train data: make predictions and compute confusion matrix
    y_pred = model.predict_classes(seq_array_train, verbose=1, batch_size=16)
    y_true = label_array_train
    #print(y_pred)
    #print(y_true)

    y_true = np.argmax(y_true, axis=1)

    print('\nParameters: sequence length: ',sys.argv[1],'  sofa_score threshold: ', sys.argv[2])

    print('\nTrain Sequences labels distribution:')
    print(pd.DataFrame(label_gen_train).value_counts())
    
    print('\nConfusion matrix:')
    cm = confusion_matrix(y_true, y_pred)
    print(cm)

    # compute precision and recall
    accuracy = accuracy_score(y_true, y_pred)
    balanced_accuracy = balanced_accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    print(' accuracy = ', accuracy, '\n balanced_accuracy = ', balanced_accuracy, '\n precision = ', precision, '\n', 'recall = ', recall)

    #------------------------
    # Test data:  make predictions and compute confusion matrix
    y_pred = model.predict_classes(seq_array_test, verbose=1, batch_size=16)
    y_true = label_array_test
    #print(y_pred)
    #print(y_true)

    y_true = np.argmax(y_true, axis=1)
    print('\nTest Sequences labels distribution:')
    print(pd.DataFrame(label_gen_test).value_counts())
    
    print('\nConfusion matrix:')
    cm = confusion_matrix(y_true, y_pred)
    print(cm)

    # compute precision and recall
    accuracy = accuracy_score(y_true, y_pred)
    balanced_accuracy = balanced_accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    print(' accuracy = ', accuracy, '\n balanced_accuracy = ', balanced_accuracy, '\n precision = ', precision, '\n', 'recall = ', recall)

    
