from gtts import gTTS
import streamlit as st
import ollama

# Fonction pour la synthèse vocale
def text_to_speech(text):
    tts = gTTS(text, lang="fr")
    audio_file = "response.mp3"
    tts.save(audio_file)
    return audio_file

# Fonction pour détecter les émotions dans le texte
def detect_emotion_from_message(message):
    if "triste" in message.lower() or "tristesse" in message.lower():
        return "tristesse"
    elif "heureux" in message.lower() or "joie" in message.lower():
        return "joie"
    elif "colère" in message.lower() or "énervé" in message.lower():
        return "colère"
    elif "surpris" in message.lower() or "étonné" in message.lower():
        return "surprise"
    else:
        return "neutre"

# Fonction pour générer une réponse personnalisée en fonction de l'émotion
def generate_response_with_emotion(model, message, emotion, config):
    # Prompt de base
    base_prompt = "Vous êtes un assistant empathique."
    if emotion == "tristesse":
        base_prompt += " Répondez avec compassion et empathie."
    elif emotion == "joie":
        base_prompt += " Répondez avec enthousiasme."
    elif emotion == "colère":
        base_prompt += " Répondez calmement et de manière apaisante."
    elif emotion == "surprise":
        base_prompt += " Répondez de manière encourageante."
    else:
        base_prompt += " Répondez normalement."

    # Ajout du contexte au modèle
    messages = [
        {"role": "system", "content": base_prompt},
        {"role": "user", "content": message}
    ]

    # Générer la réponse avec le modèle Ollama
    full_response = ""
    for response in ollama.chat(model=model, messages=messages, options=config, stream=True):
        full_response += response['message']['content']
    return full_response

# Interface Streamlit
st.title("ENSET Chatbot avec Synthèse Vocale")

# --- Options de configuration ---
st.sidebar.header("Options de configuration")

# Sélection du modèle
available_models = ["llama3.2", "qwen:0.5b"]  # Liste des modèles disponibles
selected_model = st.sidebar.selectbox("Modèle", available_models, index=0)

# Paramètres du modèle
temperature = st.sidebar.slider("Température", min_value=0.0, max_value=2.0, value=0.7, step=0.1)
top_k = st.sidebar.slider("Top-k", min_value=1, max_value=100, value=40, step=1)
top_p = st.sidebar.slider("Top-p", min_value=0.0, max_value=1.0, value=0.9, step=0.05)
max_tokens = st.sidebar.slider("Nombre maximal de tokens", min_value=1, max_value=2048, value=512, step=1)

# Entrée utilisateur
user_input = st.text_input("Entrez votre message :")
if user_input:
    # Détection de l'émotion
    emotion = detect_emotion_from_message(user_input)

    # Génération de la réponse personnalisée
    response = generate_response_with_emotion(
        selected_model,
        user_input,
        emotion,
        {"temperature": temperature, "top_k": top_k, "top_p": top_p, "num_predict": max_tokens}
    )

    # Afficher la réponse
    st.write(f"Emotion détectée : **{emotion}**")
    st.write(f"Réponse : **{response}**")

    # Synthèse vocale
    audio_file = text_to_speech(response)
    st.audio(audio_file)
