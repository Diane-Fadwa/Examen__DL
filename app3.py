import streamlit as st
import ollama

# Titre de l'application
st.title("Consultant RH Virtuel 🤝")

# Section pour l'objectif utilisateur
st.sidebar.header("Objectif de la session")
goals = ["Préparer un entretien", "Améliorer mon CV", "Réponses aux questions de carrière"]
selected_goal = st.sidebar.selectbox("Choisissez votre objectif", goals)

# Options de configuration
st.sidebar.header("Options de configuration")
available_models = ["llama3.2", "qwen:0.5b"]
selected_model = st.sidebar.selectbox("Modèle LLM", available_models, index=0)
temperature = st.sidebar.slider("Température", 0.0, 2.0, 0.7, 0.1)
top_k = st.sidebar.slider("Top-k", 1, 100, 40, 1)
top_p = st.sidebar.slider("Top-p", 0.0, 1.0, 0.9, 0.05)
max_tokens = st.sidebar.slider("Nombre maximal de tokens", 1, 2048, 512, 1)

# Vérifier la connexion au modèle LLM
try:
    test_response = ollama.chat(
        model=selected_model,
        messages=[{"role": "user", "content": "Test de connexion au modèle"}]
    )
    st.sidebar.success(f"Le modèle '{selected_model}' fonctionne correctement !")
except Exception as e:
    st.sidebar.error(f"Erreur de connexion au modèle : {e}")

# Initialiser l'historique des messages
if "messages" not in st.session_state:
    st.session_state.messages = []

# Fonction pour détecter les émotions dans les messages
def detect_emotion_from_message(message):
    """Détecte les émotions basées sur des mots-clés en contexte RH."""
    if any(word in message.lower() for word in ["stress", "nerveux", "anxieux", "doute"]):
        return "stress"
    elif any(word in message.lower() for word in ["confiant", "positif", "prêt"]):
        return "confiance"
    elif any(word in message.lower() for word in ["perdu", "confus", "difficile"]):
        return "confusion"
    else:
        return "neutre"

# Fonction pour générer des réponses personnalisées
def generate_personalized_response(emotion, response):
    """Ajoute une personnalisation en fonction de l'émotion détectée."""
    if emotion == "stress":
        return f"Je ressens une certaine nervosité. Pas de souci, voici un conseil utile : {response}"
    elif emotion == "confiance":
        return f"Vous semblez très confiant, bravo ! Continuez ainsi : {response}"
    elif emotion == "confusion":
        return f"Il semble que vous ayez besoin d'éclaircissements. Voici une explication détaillée : {response}"
    else:
        return response

# Afficher les messages existants dans la conversation
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Entrée utilisateur
prompt = st.chat_input("Posez votre question ou simulez un entretien :")
if prompt:
    # Ajouter la question de l'utilisateur dans l'interface
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Détecter l'émotion associée au message
    emotion = detect_emotion_from_message(prompt)

    # Générer une réponse avec le modèle
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""

        # Appeler le modèle LLM avec les options configurées
        for response in ollama.chat(
            model=selected_model,
            messages=st.session_state.messages,
            options={
                "temperature": temperature,
                "top_k": top_k,
                "top_p": top_p,
                "num_predict": max_tokens,
            },
            stream=True
        ):
            full_response += response["message"]["content"]
            message_placeholder.markdown(full_response + "▌")  # Animation pendant la génération
        message_placeholder.markdown(full_response)

    # Ajouter une personnalisation basée sur l'émotion
    personalized_response = generate_personalized_response(emotion, full_response)

    # Afficher et enregistrer la réponse dans l'historique
    with st.chat_message("assistant"):
        st.markdown(personalized_response)
    st.session_state.messages.append({"role": "assistant", "content": personalized_response})
